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
                compose_in_qty,
                compose_out_qty,
                compose_in_amt_src,
                compose_out_amt_src,
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

        # 6. Python: compose 初始净额（用源表 amt，后续用加工关系修正）
        df['compose_net_qty'] = df['compose_in_qty'] - df['compose_out_qty']
        df['compose_in_amt'] = df['compose_in_amt_src'].fillna(0)
        df['compose_out_amt'] = df['compose_out_amt_src'].fillna(0)
        df['compose_net_amt'] = df['compose_in_amt'] - df['compose_out_amt']
        # 第一层兜底: cost_price (系统标准成本)
        fallback_cp = ((df['compose_net_amt'].abs() < 0.001) &
                       (df['compose_net_qty'].abs() > 0.001) &
                       (df['cost_price'] > 0))
        df.loc[fallback_cp, 'compose_net_amt'] = (
            df.loc[fallback_cp, 'compose_net_qty'] * df.loc[fallback_cp, 'cost_price'])
        # 第二层兜底: avg_inbound_price (历史采购均价)
        fallback_aip = ((df['compose_net_amt'].abs() < 0.001) &
                        (df['compose_net_qty'].abs() > 0.001))
        df.loc[fallback_aip, 'compose_net_amt'] = (
            df.loc[fallback_aip, 'compose_net_qty'] * df.loc[fallback_aip, 'avg_inbound_price'])

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

        # 对于只有 compose_out 没有进货的原料，base_euc 为 0 导致 EUC 也是 0
        # 用 compose_out_amt / compose_out_qty 反推（相当于原料的单位消耗成本）
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
            self._log.info("no processing relations loaded, keeping source compose amounts")
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
        out_mask = df['compose_out_qty'] > 0
        out_with_euc = out_mask & (df['base_euc'] > 0)
        df.loc[out_with_euc, 'compose_out_amt'] = (
            df.loc[out_with_euc, 'compose_out_qty'] * df.loc[out_with_euc, 'base_euc']
        ).round(4)
        corrected_out = out_with_euc.sum()

        # ── 2. 成品 compose_in_amt = compose_in_qty × 配方单位成本 (PRIMARY) ──
        #    成品单位成本 = Σ(raw_qty / yield_qty × raw_base_euc)
        #    有加工关系的成品：全部用配方推算，不使用源表值
        #    无加工关系的成品：保留源表值（此类商品不受加工关系管理）

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
                continue  # 无加工关系 → 保留源表值

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

    _PROC_REL_API = "http://47.115.213.115:5003/api/proc-rel/export"
    _PROC_REL_CACHE = None  # 类级别缓存

    @classmethod
    def _load_processing_relations(cls):
        """加载加工关系（优先本地缓存 JSON，失败则 API 调用）。"""
        if cls._PROC_REL_CACHE is not None:
            return cls._PROC_REL_CACHE

        # 1. 尝试本地缓存
        cache_paths = [
            Path(__file__).parent.parent.parent / "data" / "processing_relations.json",
            Path("/opt/fm/proc-rel/processing_relations.json"),
            Path("data/processing_relations.json"),
        ]
        for p in cache_paths:
            if p.exists():
                try:
                    data = json.loads(p.read_text())
                    cls._PROC_REL_CACHE = data.get("relations", [])
                    return cls._PROC_REL_CACHE
                except (json.JSONDecodeError, KeyError):
                    continue

        # 2. API 调用
        try:
            resp = requests.get(cls._PROC_REL_API, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                cls._PROC_REL_CACHE = data.get("relations", [])
                return cls._PROC_REL_CACHE
        except Exception:
            pass

        cls._PROC_REL_CACHE = []
        return cls._PROC_REL_CACHE

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
