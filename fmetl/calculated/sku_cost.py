"""
t_calc_sku_cost — SKU 有效单位成本 (v10 Python 重写)

v10 算法（加权平均，含期初库存）:
  cost_amt = init_stock_amt + self_receive_amt + compose_net_amt + bom_alloc_amt
  cost_qty = init_stock_qty + self_receive_qty + compose_net_qty + bom_alloc_qty
  euc = cost_amt / cost_qty

  - init_stock: 昨天 t_calc_stock.end_stock（首日: atomic_inventory 源表值）
  - compose: 加工关系推算 compose_in_amt，compose_out_amt = qty × euc（价值守恒）
  - 不用 cost_price
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from pathlib import Path

import duckdb
import requests
from ..connectors import DuckDBStore
from ..utils import get_logger


class SkuCostCalculator:
    TARGET_TABLE = "t_calc_sku_cost"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("SkuCostCalculator")

    def run(self) -> None:
        self._log.info("calculating SKU effective unit cost (v10 Python) ...")
        conn = self._duck._conn

        # 1. 从 t_atomic_wide 取基础数据
        wide_df = conn.execute("""
            SELECT
                store_id,
                business_date,
                article_id,
                self_receive_qty,
                self_receive_amt,
                sale_qty,
                know_lost_qty,
                day_clear,
                init_stock_qty_src,
                init_stock_amt_src,
                avg_inbound_price,
                cost_price
            FROM t_atomic_wide
        """).df()

        if wide_df.empty:
            self._log.warning("t_atomic_wide is empty, creating empty t_calc_sku_cost")
            self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
            self._duck.execute(f"""
                CREATE TABLE {self.TARGET_TABLE} (
                    store_id VARCHAR, business_date VARCHAR, article_id VARCHAR,
                    total_cost_amt DOUBLE, cost_qty DOUBLE,
                    effective_unit_cost DOUBLE, cost_source VARCHAR,
                    self_inbound_qty DOUBLE, self_inbound_amt DOUBLE,
                    compose_net_qty DOUBLE, compose_net_amt DOUBLE,
                    compose_in_amt DOUBLE, compose_out_amt DOUBLE,
                    compose_in_qty DOUBLE, compose_out_qty DOUBLE,
                    bom_alloc_amt DOUBLE, bom_alloc_qty DOUBLE,
                    init_stock_qty DOUBLE, init_stock_amt DOUBLE,
                    avg_inbound_price DOUBLE, is_first_day INTEGER
                )
            """)
            return

        # 2. 从 t_calc_bom_alloc 取 BOM 分摊（按 sub 聚合, 用子品单位 qty）
        bom_df = conn.execute("""
            SELECT
                store_id,
                business_date,
                sub_article_id     AS article_id,
                SUM(bom_alloc_amt) AS bom_alloc_amt,
                SUM(bom_alloc_qty_sub) AS bom_alloc_qty
            FROM t_calc_bom_alloc
            GROUP BY store_id, business_date, sub_article_id
        """).df()

        # 3. 从 t_calc_stock 取前一营业日期末库存（MAX business_date < 当天）
        prev_df = None
        try:
            prev_df = conn.execute("""
                WITH stock_pairs AS (
                    SELECT DISTINCT store_id, article_id, business_date
                    FROM t_atomic_wide
                ),
                prev_match AS (
                    SELECT sp.store_id, sp.article_id, sp.business_date,
                           MAX(cs.business_date) AS prev_biz_date
                    FROM stock_pairs sp
                    INNER JOIN t_calc_stock cs
                        ON sp.store_id = cs.store_id
                        AND sp.article_id = cs.article_id
                        AND cs.business_date < sp.business_date
                    GROUP BY sp.store_id, sp.article_id, sp.business_date
                ),
                prev_stock AS (
                    SELECT pm.store_id, pm.article_id, pm.business_date,
                           cs.end_stock_qty AS prev_end_qty,
                           cs.end_stock_amt AS prev_end_amt
                    FROM prev_match pm
                    INNER JOIN t_calc_stock cs
                        ON pm.store_id = cs.store_id
                        AND pm.article_id = cs.article_id
                        AND pm.prev_biz_date = cs.business_date
                )
                SELECT * FROM prev_stock
            """).df()
            if prev_df.empty:
                prev_df = None
        except (duckdb.CatalogException, Exception):
            self._log.info("no t_calc_stock from prior runs — all SKUs are first-day")

        # 4. Python: merge BOM
        df = wide_df.merge(bom_df, on=['store_id', 'business_date', 'article_id'],
                           how='left')
        df['bom_alloc_qty'] = df['bom_alloc_qty'].fillna(0)
        df['bom_alloc_amt'] = df['bom_alloc_amt'].fillna(0)

        # 5. Python: 计算 init_stock（前一营业日期末 或 首日源表值）
        if prev_df is not None and not prev_df.empty:
            df = df.merge(prev_df,
                          on=['store_id', 'article_id', 'business_date'],
                          how='left')
            df['init_stock_qty'] = df['prev_end_qty'].fillna(
                df['init_stock_qty_src']).clip(lower=0)
            df['init_stock_amt'] = df['prev_end_amt'].fillna(
                df['init_stock_amt_src']).clip(lower=0)
            df['is_first_day'] = df['prev_end_qty'].isna().astype(int)
        else:
            df['init_stock_qty'] = df['init_stock_qty_src'].clip(lower=0)
            df['init_stock_amt'] = df['init_stock_amt_src'].clip(lower=0)
            df['is_first_day'] = 1

        # 5.5 加载盘点实际库存 (用于 compose 数量推导)
        inv_detail = conn.execute("""
            SELECT store_id, business_date, article_id,
                   actual_stock_qty
            FROM atomic_inventory_detail
            WHERE actual_stock_qty > 0
        """).df()
        if not inv_detail.empty:
            df = df.merge(inv_detail,
                          on=['store_id', 'business_date', 'article_id'],
                          how='left')
            df['actual_stock_qty'] = df['actual_stock_qty'].fillna(0)
        else:
            df['actual_stock_qty'] = 0.0

        # 6. 从业务行为推导 compose 数量（完全不依赖源表 compose_in_qty / compose_out_qty）
        #    成品 compose_in_qty = max(0, sale + loss - init - recv)
        #       - 日清品(day_clear='0'): init 通常≈0, compose_in ≈ sale + loss
        #       - 有盘点(actual_stock_qty>0): compose_in = max(0, actual + sale + loss - init - recv)
        #       - 成品没有直接收货(recv=0): 销售和损耗全来自加工产出
        #       - 先消耗期初库存，不够的部分由加工产出补充
        #    原料 compose_out_qty = Σ(成品 compose_in_qty × raw_qty / yield_qty)
        #       - 从加工关系的配方比例反推原料消耗量
        #    compose 金额在 Step 8 由加工关系推算
        df['compose_in_qty'] = 0.0
        df['compose_out_qty'] = 0.0

        relations_qty = self._load_processing_relations()
        if relations_qty:
            from collections import defaultdict
            # finished → [(raw_sku, raw_qty, yield_qty), ...]
            proc_map_qty = defaultdict(list)
            # raw → [(finished_sku, raw_qty, yield_qty), ...]
            raw_to_finished = defaultdict(list)
            for rel in relations_qty:
                f = rel["finished_sku"]
                r = rel["raw_sku"]
                rq = float(rel["raw_qty"])
                yq = float(rel["yield_qty"])
                proc_map_qty[f].append({"raw_sku": r, "raw_qty": rq, "yield_qty": yq})
                raw_to_finished[r].append({"finished_sku": f, "raw_qty": rq, "yield_qty": yq})

            finished_set = set(proc_map_qty.keys())
            derived_in_count = 0
            derived_out_count = 0

            # (a) 成品 compose_in_qty = max(0, sale + loss - init - recv)
            #     若有盘点实绩: compose_in_qty = max(0, actual + sale + loss - init - recv)
            for idx in df.index:
                article_id = str(df.at[idx, 'article_id'])
                if article_id not in finished_set:
                    continue
                sale = float(df.at[idx, 'sale_qty'])
                loss = float(df.at[idx, 'know_lost_qty'])
                init = float(df.at[idx, 'init_stock_qty'])
                recv = float(df.at[idx, 'self_receive_qty'])
                actual = float(df.at[idx, 'actual_stock_qty'])
                if actual > 0:
                    # 盘点场景: 已知期末库存 = actual, 反推生产量
                    derived_in = max(0.0, round(actual + sale + loss - init - recv, 4))
                else:
                    derived_in = max(0.0, round(sale + loss - init - recv, 4))
                if derived_in > 0:
                    df.at[idx, 'compose_in_qty'] = derived_in
                    derived_in_count += 1

            # (b) 原料 compose_out_qty = Σ(成品 compose_in × raw_qty / yield_qty)
            for (biz_date, store_id), grp in df.groupby(['business_date', 'store_id']):
                for idx in grp.index:
                    article_id = str(df.at[idx, 'article_id'])
                    if article_id not in raw_to_finished:
                        continue
                    total_out = 0.0
                    for mapping in raw_to_finished[article_id]:
                        finished = mapping['finished_sku']
                        raw_qty = mapping['raw_qty']
                        yield_qty = mapping['yield_qty']
                        if yield_qty <= 0:
                            continue
                        finished_rows = df[
                            (df['article_id'] == finished) &
                            (df['business_date'] == biz_date) &
                            (df['store_id'] == store_id)
                        ]
                        if len(finished_rows) > 0:
                            finished_cin = float(finished_rows['compose_in_qty'].values[0])
                            if finished_cin > 0:
                                total_out += finished_cin * raw_qty / yield_qty
                    if total_out > 0:
                        df.at[idx, 'compose_out_qty'] = round(total_out, 4)
                        derived_out_count += 1

            self._log.info(
                f"compose qty derived from biz events: "
                f"{derived_in_count} compose_in rows, "
                f"{derived_out_count} compose_out rows, "
                f"{len(finished_set)} finished SKUs in PR"
            )

        df['compose_net_qty'] = df['compose_in_qty'] - df['compose_out_qty']
        df['compose_in_amt'] = 0.0
        df['compose_out_amt'] = 0.0
        df['compose_net_amt'] = 0.0

        # 7. 计算 base EUC（不含 compose，因为 compose_out 在公式中自抵消）
        #    base_euc = (init + recv + bom) / (init_q + recv_q + bom_q)
        #    原料的 compose_out_amt = compose_out_qty × base_euc
        #    成品的 compose_in_amt = compose_in_qty × Σ(raw_qty / yield_qty × raw_base_euc)
        df['base_cost_amt'] = (df['init_stock_amt'] + df['self_receive_amt']
                               + df['bom_alloc_amt'])
        df['base_cost_qty'] = (df['init_stock_qty'] + df['self_receive_qty']
                               + df['bom_alloc_qty'])

        df['base_euc'] = 0.0
        base_mask = df['base_cost_qty'] > 0
        df.loc[base_mask, 'base_euc'] = (
            df.loc[base_mask, 'base_cost_amt'] / df.loc[base_mask, 'base_cost_qty']
        )

        # 8. 加工关系修正 compose_in_amt / compose_out_amt
        #    使用 base_euc（不含 compose 影响）推算 compose 金额
        df = self._apply_compose_corrections(df)

        # 9. 用修正后的 compose 金额重新计算最终 EUC
        df['cost_amt'] = df['base_cost_amt'] + df['compose_net_amt']
        df['cost_qty'] = df['base_cost_qty'] + df['compose_net_qty']

        df['effective_unit_cost'] = 0.0
        mask = df['cost_qty'] > 0
        df.loc[mask, 'effective_unit_cost'] = (
            df.loc[mask, 'cost_amt'] / df.loc[mask, 'cost_qty']
        )

        # 对于只有 compose_out 没有进货的原料
        # base_euc > 0 → compose_out_amt = qty × base_euc (价值守恒) → euc = base_euc ✅
        # base_euc = 0 → compose_out_amt = 0 → euc = 0 → 进入 EUC 兜底链
        # 次日 base_euc > 0 后价值守恒自动生效
        no_base = (df['base_cost_qty'] <= 0) & (df['compose_out_qty'] > 0)
        df.loc[no_base, 'effective_unit_cost'] = (
            df.loc[no_base, 'compose_out_amt'] / df.loc[no_base, 'compose_out_qty']
        )

        # cost_qty=0 时沿 (store_id, article_id) 向前继承昨日 euc
        df = df.sort_values(['store_id', 'article_id', 'business_date'])
        df['effective_unit_cost'] = (
            df.groupby(['store_id', 'article_id'])['effective_unit_cost']
            .transform(lambda x: x.replace(0, float('nan')).ffill().fillna(0))
        )

        # 仍为0的(首日也无供给)回退到avg_inbound_price
        fallback_aip = (df['effective_unit_cost'] == 0) & (df['avg_inbound_price'] > 0)
        df.loc[fallback_aip, 'effective_unit_cost'] = df.loc[fallback_aip, 'avg_inbound_price']

        # 仍为0的回退到 加工关系推算成本 (原料进货价 × 配方用量 / 产出)
        fallback_pr = self._apply_processing_relation_fallback(df)

        df['cost_source'] = 'V10_WEIGHTED_AVG'
        inherited = ((~mask) & (df['effective_unit_cost'] > 0) &
                     (~fallback_aip) & (~fallback_pr))
        df.loc[inherited, 'cost_source'] = 'V10_INHERITED_EUC'
        df.loc[fallback_aip, 'cost_source'] = 'V10_AVG_INBOUND_FALLBACK'
        df.loc[fallback_pr, 'cost_source'] = 'V10_PROCESSING_RELATION'

        # 清理临时列
        for tmp_col in ['_pr_applied', '_compose_corrected']:
            if tmp_col in df.columns:
                df.drop(columns=[tmp_col], inplace=True)

        # 恢复原始排序
        df = df.sort_index()

        # 10. 写出结果（分区覆盖，保留历史数据）
        sel_cols = f"""
            SELECT
                store_id,
                business_date,
                article_id,
                cost_amt                AS total_cost_amt,
                cost_qty,
                effective_unit_cost,
                cost_source,
                self_receive_qty        AS self_inbound_qty,
                self_receive_amt        AS self_inbound_amt,
                compose_net_qty,
                compose_net_amt,
                compose_in_amt,
                compose_out_amt,
                compose_in_qty,
                compose_out_qty,
                bom_alloc_amt,
                bom_alloc_qty,
                init_stock_qty,
                init_stock_amt,
                avg_inbound_price,
                is_first_day
            FROM df
        """
        # 首次建表（空结构）
        conn.execute(f"CREATE TABLE IF NOT EXISTS {self.TARGET_TABLE} AS {sel_cols} LIMIT 0")
        # 按日期分区覆盖
        date_min, date_max = df['business_date'].min(), df['business_date'].max()
        conn.execute(f"DELETE FROM {self.TARGET_TABLE} WHERE business_date BETWEEN '{date_min}' AND '{date_max}'")
        conn.execute(f"INSERT INTO {self.TARGET_TABLE} {sel_cols}")

        rows = self._duck.row_count(self.TARGET_TABLE)
        first_day_cnt = int(df['is_first_day'].sum())
        self._log.info(f"t_calc_sku_cost: {rows} rows, first_day={first_day_cnt}")

        # 统计 euc 分布
        pos = df[df['effective_unit_cost'] > 0]
        if len(pos) > 0:
            self._log.info(
                f"euc stats: n={len(pos)}, "
                f"min={pos['effective_unit_cost'].min():.4f}, "
                f"median={pos['effective_unit_cost'].median():.4f}, "
                f"max={pos['effective_unit_cost'].max():.4f}"
            )

        # 统计 cost_source 分布
        src_counts = df['cost_source'].value_counts()
        for src, cnt in src_counts.items():
            self._log.info(f"  cost_source={src}: {cnt} rows")

        # 零成本 SKU 警告
        zero_cost = df[df['effective_unit_cost'] == 0]
        if len(zero_cost) > 0:
            zero_skus = zero_cost['article_id'].unique()
            self._log.warning(
                f"ZERO EUC after all fallbacks: {len(zero_cost)} rows, "
                f"{len(zero_skus)} unique SKUs, "
                f"sample SKUs: {list(zero_skus[:5])}"
            )

    # ── 加工关系修正 compose_in_amt / compose_out_amt ──────────────

    def _apply_compose_corrections(self, df):
        """用加工关系作为 compose 金额的主要计算来源。

        加工关系逻辑: raw_qty 单位原料 → yield_qty 单位成品。

        策略:
        - 成品 compose_in_amt = compose_in_qty × Σ(raw_qty / yield_qty × raw_base_euc)
          即：按配方从原料成本推算成品成本（PRIMARY，不用源表值）
        - 原料 compose_out_amt = compose_out_qty × base_euc (价值守恒)
        """
        relations = self._load_processing_relations()
        if not relations:
            self._log.info("no processing relations loaded, compose amounts remain 0")
            df['_compose_corrected'] = False
            return df

        # 构建加工关系索引: finished_sku → [(raw_sku, raw_qty, yield_qty), ...]
        from collections import defaultdict
        proc_map = defaultdict(list)
        for rel in relations:
            finished = rel["finished_sku"]
            raw = rel["raw_sku"]
            proc_map[finished].append({
                "raw_sku": raw,
                "raw_qty": float(rel["raw_qty"]),
                "yield_qty": float(rel["yield_qty"]),
            })

        df['_compose_corrected'] = False
        corrected_out = 0
        corrected_in = 0

        # ── 1. 原料 compose_out_amt = qty × base_euc (价值守恒) ──
        #    base_euc > 0 → 价值守恒计算
        #    base_euc = 0 → compose_out_amt 保持 0（首日或 EUC=0 SKU）
        #    EUC 兜底链会在后续步骤提供估算，次日 base_euc>0 后自动修复
        out_mask = df['compose_out_qty'] > 0
        out_with_euc = out_mask & (df['base_euc'] > 0)
        df.loc[out_with_euc, 'compose_out_amt'] = (
            df.loc[out_with_euc, 'compose_out_qty'] * df.loc[out_with_euc, 'base_euc']
        ).round(4)
        corrected_out = out_with_euc.sum()

        # ── 2. 成品 compose_in_amt = compose_in_qty × 配方单位成本 ──
        #    成品单位成本 = Σ(raw_qty / yield_qty × raw_base_euc)
        #    有加工关系 + 配方完整 → 配方推算
        #    无加工关系 → compose_in_amt 保持 0（EUC 兜底链提供估算）
        #    有加工关系但原料 euc 不全 → compose_in_amt 保持 0（次日原料修复后自动修复）

        # 构建原料 base_euc lookup（按 date × store × article）
        euc_lookup = {}
        for (biz_date, store_id), grp in df.groupby(['business_date', 'store_id']):
            for _, row in grp.iterrows():
                euc_lookup[(biz_date, store_id, row['article_id'])] = row['base_euc']

        in_mask = df['compose_in_qty'] > 0
        in_indices = df[in_mask].index

        for idx in in_indices:
            article_id = str(df.at[idx, 'article_id'])
            if article_id not in proc_map:
                # 无加工关系 → compose_in_amt 保持 0
                # EUC 兜底链 (cost_price → current_price×0.40) 会在后续步骤提供估算
                self._log.debug(f"compose_in SKU {article_id} 无加工关系, compose_in_amt=0")
                continue

            store_id = str(df.at[idx, 'store_id'])
            biz_date = str(df.at[idx, 'business_date'])
            compose_in_qty = float(df.at[idx, 'compose_in_qty'])
            recipe_list = proc_map[article_id]

            # 配方单位成本 = Σ(raw_qty / yield_qty × raw_euc)
            finished_unit_cost = 0.0
            all_raw_found = True
            for recipe in recipe_list:
                raw_sku = recipe["raw_sku"]
                raw_qty = recipe["raw_qty"]
                yield_qty = recipe["yield_qty"]
                if yield_qty <= 0:
                    all_raw_found = False
                    continue

                # 同店同日原料 base_euc
                raw_euc = euc_lookup.get((biz_date, store_id, raw_sku), 0)
                if raw_euc <= 0:
                    # 跨店回退：同日任意门店
                    raw_euc_vals = [
                        v for (d, s, a), v in euc_lookup.items()
                        if d == biz_date and a == raw_sku and v > 0
                    ]
                    if raw_euc_vals:
                        raw_euc = sum(raw_euc_vals) / len(raw_euc_vals)
                    else:
                        all_raw_found = False

                if raw_euc > 0:
                    finished_unit_cost += (raw_qty / yield_qty) * raw_euc

            if finished_unit_cost > 0 and all_raw_found:
                new_amt = round(compose_in_qty * finished_unit_cost, 4)
                old_amt = df.at[idx, 'compose_in_amt']
                df.at[idx, 'compose_in_amt'] = new_amt
                df.at[idx, '_compose_corrected'] = True
                corrected_in += 1
                if abs(new_amt - old_amt) > 0.5:
                    self._log.info(
                        f"compose_in_amt(corrected): {article_id} "
                        f"qty={compose_in_qty} old={old_amt:.2f} new={new_amt:.2f} "
                        f"unit_cost={finished_unit_cost:.4f}"
                    )

        # 重新计算 compose_net_amt
        df['compose_net_amt'] = df['compose_in_amt'] - df['compose_out_amt']

        self._log.info(
            f"compose corrections: {corrected_out} compose_out (value conservation), "
            f"{corrected_in} compose_in (recipe-based), "
            f"{len(proc_map)} finished SKU recipes loaded"
        )

        return df

    # ── 加工关系成本推算（兜底: EUC=0 的成品）──────────────────────

    _PROC_REL_API_URLS = [
        "http://47.115.213.115:8080/api/proc-rel/export",   # nginx 代理（外部可达）
        "http://127.0.0.1:5003/api/proc-rel/export",         # 服务器本地直连
    ]
    _PROC_REL_CACHE = None  # 类级别缓存

    @classmethod
    def _load_processing_relations(cls):
        """加载加工关系。

        策略（每次 ETL 运行都尝试拉取最新数据）:
        1. 远程 API（通过 nginx :8080 或本地 :5003）—— 保证数据最新
        2. 本地缓存 JSON —— 网络不可达时的兜底
        3. API 成功后自动写入本地缓存
        """
        if cls._PROC_REL_CACHE is not None:
            return cls._PROC_REL_CACHE

        relations = None

        # 1. 远程 API（优先，保证数据最新）
        for url in cls._PROC_REL_API_URLS:
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    raw_relations = data.get("relations", [])
                    # 过滤自身关系 (raw_sku == finished_sku, 1:1 无意义)
                    relations = [r for r in raw_relations
                                 if str(r.get("raw_sku", "")) != str(r.get("finished_sku", ""))]
                    cls._PROC_REL_CACHE = relations
                    # 自动更新本地缓存（保存过滤后的数据）
                    cls._save_local_cache({"relations": relations})
                    return cls._PROC_REL_CACHE
            except Exception:
                continue

        # 2. 本地缓存兜底
        cache_paths = [
            Path(__file__).parent.parent.parent / "data" / "processing_relations.json",
            Path("/opt/fm/proc-rel/processing_relations.json"),
            Path("data/processing_relations.json"),
        ]
        for p in cache_paths:
            if p.exists():
                try:
                    data = json.loads(p.read_text())
                    raw_relations = data.get("relations", [])
                    # 同样过滤自身关系
                    relations = [r for r in raw_relations
                                 if str(r.get("raw_sku", "")) != str(r.get("finished_sku", ""))]
                    cls._PROC_REL_CACHE = relations
                    return cls._PROC_REL_CACHE
                except (json.JSONDecodeError, KeyError):
                    continue

        cls._PROC_REL_CACHE = []
        return cls._PROC_REL_CACHE

    @classmethod
    def _save_local_cache(cls, data):
        """API 拉取成功后写入本地缓存文件。"""
        cache_path = Path(__file__).parent.parent.parent / "data" / "processing_relations.json"
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        except Exception:
            pass  # 写入失败不阻塞 ETL

    def _apply_processing_relation_fallback(self, df):
        """加工关系成本推算: Σ(原料用量 × 原料euc) / 产出数量。
        返回 fallback mask (哪些行被此兜底覆盖)。"""
        relations = self._load_processing_relations()
        if not relations:
            df['_pr_applied'] = False
            return df['_pr_applied']

        # 按 finished_sku 建立索引: {finished_sku: [{raw_sku, raw_qty, yield_qty}, ...]}
        from collections import defaultdict
        proc_map = defaultdict(list)
        for rel in relations:
            proc_map[rel["finished_sku"]].append(rel)

        # 对每个 euc=0 的成品 SKU，查找加工关系推算成本
        euc_zero_idx = df[df['effective_unit_cost'] == 0].index
        if len(euc_zero_idx) == 0:
            df['_pr_applied'] = False
            return df['_pr_applied']

        df['_pr_applied'] = False
        processed = 0

        for idx in euc_zero_idx:
            article_id = df.at[idx, 'article_id']
            if article_id not in proc_map:
                continue

            recipe_list = proc_map[article_id]
            # 同一成品的多个原料
            total_raw_cost = 0.0
            total_yield_qty = 0.0
            has_data = False

            for recipe in recipe_list:
                raw_sku = recipe["raw_sku"]
                raw_qty = float(recipe["raw_qty"])
                yield_qty = float(recipe.get("yield_qty", 1))

                # 查找原料在同一 business_date 的 euc（可能已计算好）
                raw_rows = df[(df['article_id'] == raw_sku) &
                              (df['business_date'] == df.at[idx, 'business_date'])]
                if len(raw_rows) > 0:
                    raw_euc = raw_rows['effective_unit_cost'].values[0]
                    if raw_euc > 0:
                        total_raw_cost += raw_qty * raw_euc
                        total_yield_qty = yield_qty  # 取第一个原料的产出（同一成品）
                        has_data = True

            if has_data and total_yield_qty > 0:
                df.at[idx, 'effective_unit_cost'] = total_raw_cost / total_yield_qty
                df.at[idx, '_pr_applied'] = True
                processed += 1

        if processed > 0:
            self._log.info(
                f"V10_PROCESSING_RELATION: {processed} rows "
                f"from {len(proc_map)} finished SKU recipes"
            )

        return df['_pr_applied']
