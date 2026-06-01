"""
t_calc_bom_alloc — BOM 分摊事实表（v9 Σ总权重算法 + Python共享组识别）

核心逻辑:
    1. 消耗权重 = 消耗数量 × 销售原价
    2. 拆分需求权重 = 消耗权重 - 自己进货权重 (Type A) 或 消耗权重 (Type B/C)
    3. 识别parent共享组（子集关系）
    4. Σ总权重 = 共享组所有subs的拆分需求权重之和
    5. 分配占比 = 拆分需求权重 / Σ总权重
    6. bom_alloc_amt = 分配占比 × parent组进货额

parent共享组识别（Python处理）:
    如果 parent_B.subs ⊆ parent_A.subs，则合并为一个共享组，
    组内Σ总权重和parent组进货额统一计算。
"""

from __future__ import annotations

import duckdb
from ..connectors import DuckDBStore
from ..utils import get_logger


class BomAllocCalculator:
    TARGET_TABLE = "t_calc_bom_alloc"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("BomAllocCalculator")
        self._conn = duck._conn

    def run(self) -> None:
        self._log.info("calculating BOM allocation (v9 Σ总权重 + Python共享组识别) ...")
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")

        # ── Step 1: 获取基础数据 ─────────────────────────────────────
        # 1.1 BOM关系（article_id != sale_article_id）
        bom_relations = self._conn.execute("""
            SELECT
                store_id,
                business_date,
                article_id                          AS parent_article_id,
                sale_article_id                     AS sub_article_id,
                inbound_qty                         AS parent_inbound_qty,
                inbound_amount                      AS parent_inbound_amount
            FROM atomic_receive_sale
            WHERE article_id != sale_article_id
              AND store_id IS NOT NULL
              AND article_id IS NOT NULL
              AND sale_article_id IS NOT NULL
        """).fetchall()

        if not bom_relations:
            self._log.warning("无BOM关系数据，创建空表")
            self._create_empty_table()
            return

        # ── Step 2: 构建parent->subs映射 ───────────────────────────────
        # 注意：atomic_receive_sale每个parent-sub关系都记录完整parent进货额
        # 需要去重，不能简单累加
        parent_subs = {}  # {parent_id: set(sub_ids)}
        parent_info = {}  # {parent_id: {inbound_amt, inbound_qty}}

        for row in bom_relations:
            store_id, date, parent_id, sub_id, qty, amt = row
            key = (store_id, date)
            if key not in parent_subs:
                parent_subs[key] = {}
                parent_info[key] = {}

            if parent_id not in parent_subs[key]:
                parent_subs[key][parent_id] = set()
                parent_info[key][parent_id] = {'amt': None, 'qty': None}

            parent_subs[key][parent_id].add(sub_id)
            # 用第一次看到的值，不要累加（因为每个关系都记录完整值）
            if parent_info[key][parent_id]['amt'] is None:
                parent_info[key][parent_id]['amt'] = amt if amt else 0
                parent_info[key][parent_id]['qty'] = qty if qty else 0

        # ── Step 3: 识别parent共享组（子集关系）───────────────────────
        # 共享组: [{parents: [p1, p2], subs: union_subs}]
        shared_groups = {}  # {(store, date): [{parents, subs, total_amt}]}
        parent_to_group = {}  # {(store, date, parent_id): group_idx}

        for key, parents_dict in parent_subs.items():
            shared_groups[key] = []
            parent_to_group[key] = {}
            parent_list = list(parents_dict.keys())

            for i, p1 in enumerate(parent_list):
                for j, p2 in enumerate(parent_list):
                    if i >= j:
                        continue

                    p1_subs = parents_dict[p1]
                    p2_subs = parents_dict[p2]

                    # 检查子集关系
                    if p2_subs.issubset(p1_subs):
                        # p2 ⊆ p1，合并
                        self._log.info(f"发现共享组: {p2}.subs ⊆ {p1}.subs")
                        group = {
                            'parents': [p1, p2],
                            'subs': p1_subs.union(p2_subs),
                            'total_amt': parent_info[key][p1]['amt'] + parent_info[key][p2]['amt']
                        }
                        shared_groups[key].append(group)
                        parent_to_group[key][p1] = len(shared_groups[key]) - 1
                        parent_to_group[key][p2] = len(shared_groups[key]) - 1

                    elif p1_subs.issubset(p2_subs):
                        # p1 ⊆ p2，合并
                        self._log.info(f"发现共享组: {p1}.subs ⊆ {p2}.subs")
                        group = {
                            'parents': [p2, p1],
                            'subs': p1_subs.union(p2_subs),
                            'total_amt': parent_info[key][p2]['amt'] + parent_info[key][p1]['amt']
                        }
                        shared_groups[key].append(group)
                        parent_to_group[key][p1] = len(shared_groups[key]) - 1
                        parent_to_group[key][p2] = len(shared_groups[key]) - 1

        # ── Step 4: 获取sub数据（销售+损耗+价格）───────────────────────
        sub_data = self._conn.execute("""
            SELECT
                s.store_id,
                s.business_date,
                s.article_id                        AS sub_article_id,
                COALESCE(s.sale_qty, 0)             AS sale_qty,
                COALESCE(p.original_price, 0)       AS list_price,
                COALESCE(l.know_lost_qty, 0)        AS know_lost_qty
            FROM (SELECT store_id, business_date, article_id, SUM(sale_qty) AS sale_qty FROM atomic_sales GROUP BY store_id, business_date, article_id) s
            LEFT JOIN (SELECT store_id, business_date, article_id, SUM(know_lost_qty) AS know_lost_qty FROM atomic_loss GROUP BY store_id, business_date, article_id) l
              ON l.store_id = s.store_id AND l.business_date = s.business_date AND l.article_id = s.article_id
            LEFT JOIN (SELECT store_id, business_date, article_id, AVG(original_price) AS original_price FROM atomic_price GROUP BY store_id, business_date, article_id) p
              ON p.store_id = s.store_id AND p.business_date = s.business_date AND p.article_id = s.article_id
        """).fetchall()

        # 构建sub数据查找表
        sub_lookup = {}  # {(store, date, sub_id): {sale_qty, list_price, know_lost_qty}}
        for row in sub_data:
            store, date, sub_id, sale_qty, list_price, know_lost_qty = row
            sub_lookup[(store, date, sub_id)] = {
                'sale_qty': sale_qty,
                'list_price': list_price,
                'know_lost_qty': know_lost_qty
            }

        # ── Step 5: 获取自己进货数据（Type A）───────────────────────────
        self_inbound = self._conn.execute("""
            SELECT
                store_id,
                business_date,
                article_id,
                SUM(inbound_qty)                    AS self_inbound_qty,
                SUM(inbound_amount)                 AS self_inbound_amt
            FROM atomic_receive_sale
            WHERE article_id = sale_article_id
            GROUP BY store_id, business_date, article_id
        """).fetchall()

        self_inbound_lookup = {}  # {(store, date, article_id): {qty, amt}}
        for row in self_inbound:
            store, date, article_id, qty, amt = row
            self_inbound_lookup[(store, date, article_id)] = {'qty': qty, 'amt': amt}

        # ── Step 6: 计算每个sub的拆分需求权重 ────────────────────────
        sub_weights = {}  # {(store, date, sub_id): {weight, is_type_a}}

        for key, parents_dict in parent_subs.items():
            # 收集所有subs
            all_subs = set()
            for parent_id, subs in parents_dict.items():
                all_subs.update(subs)

            for sub_id in all_subs:
                sub_key = (key[0], key[1], sub_id)
                sub_info = sub_lookup.get(sub_key, {'sale_qty': 0, 'list_price': 0, 'know_lost_qty': 0})
                self_info = self_inbound_lookup.get(sub_key, {'qty': 0, 'amt': 0})

                sale_qty = sub_info['sale_qty']
                list_price = sub_info['list_price']
                know_lost_qty = sub_info['know_lost_qty']
                self_inbound_qty = self_info['qty']

                consume_qty = sale_qty + know_lost_qty
                consume_weight = consume_qty * list_price
                self_inbound_weight = self_inbound_qty * list_price

                is_type_a = self_inbound_qty > 0
                if is_type_a:
                    split_need_weight = consume_weight - self_inbound_weight
                else:
                    split_need_weight = consume_weight

                sub_weights[sub_key] = {
                    'weight': split_need_weight,
                    'is_type_a': is_type_a,
                    'self_inbound_qty': self_inbound_qty,
                    'self_inbound_amt': self_info['amt']
                }

        # ── Step 7: 计算共享组Σ总权重和分配金额 ────────────────────────
        # 统计变量（用于 log）
        store_date_keys = set()
        results = []

        for key, groups in shared_groups.items():
            for group in groups:
                group_subs = group['subs']
                group_parents = group['parents']
                group_total_amt = group['total_amt']

                # 计算Σ总权重
                group_total_weight = sum(
                    sub_weights.get((key[0], key[1], s), {}).get('weight', 0)
                    for s in group_subs
                )

                # 计算每个sub的分配金额
                for sub_id in group_subs:
                    sub_key = (key[0], key[1], sub_id)
                    weight_info = sub_weights.get(sub_key, {})
                    sub_info = sub_lookup.get(sub_key, {})
                    split_need_weight = weight_info.get('weight', 0)

                    if group_total_weight > 0:
                        alloc_ratio = split_need_weight / group_total_weight
                        bom_alloc_amt = alloc_ratio * group_total_amt
                    else:
                        alloc_ratio = 0
                        bom_alloc_amt = 0

                    # 衍生字段
                    sale_qty_val = sub_info.get('sale_qty', 0)
                    know_lost_qty_val = sub_info.get('know_lost_qty', 0)
                    list_price_val = sub_info.get('list_price', 0)
                    consume_qty_val = sale_qty_val + know_lost_qty_val
                    consume_weight_val = consume_qty_val * list_price_val
                    self_inbound_qty_val = weight_info.get('self_inbound_qty', 0)
                    self_inbound_weight_val = self_inbound_qty_val * list_price_val
                    is_type_a_val = weight_info.get('is_type_a', False)
                    split_need_qty_val = (
                        consume_qty_val - self_inbound_qty_val
                        if is_type_a_val else consume_qty_val
                    )
                    bom_alloc_qty_val = split_need_qty_val

                    results.append({
                        'store_id': key[0],
                        'business_date': key[1],
                        'parent_article_id': group_parents[0],
                        'sub_article_id': sub_id,
                        'parent_inbound_qty': parent_info[key][group_parents[0]]['qty'],
                        'parent_inbound_amt': group_total_amt,
                        'parent_unit_price': (
                            group_total_amt / parent_info[key][group_parents[0]]['qty']
                            if parent_info[key][group_parents[0]]['qty'] > 0 else 0
                        ),
                        'sale_qty': sale_qty_val,
                        'know_lost_qty': know_lost_qty_val,
                        'list_price': list_price_val,
                        'consume_qty': consume_qty_val,
                        'consume_weight': consume_weight_val,
                        'self_inbound_qty': self_inbound_qty_val,
                        'self_inbound_amt': weight_info.get('self_inbound_amt', 0),
                        'self_inbound_weight': self_inbound_weight_val,
                        'is_type_a': 1 if is_type_a_val else 0,
                        'split_need_weight': split_need_weight,
                        'split_need_qty': split_need_qty_val,
                        'bom_alloc_qty': bom_alloc_qty_val,
                        'group_total_weight': group_total_weight,
                        'alloc_ratio': alloc_ratio,
                        'bom_alloc_amt': bom_alloc_amt,
                        'dressing_rate': alloc_ratio,
                        'cost_rate_effective': 0.0,
                        'sub_qty_actual': consume_qty_val,
                        'sub_qty_source': 'SALES+LOSS',
                        'sub_alloc_amt': bom_alloc_amt,
                        'sub_unit_cost': (
                            bom_alloc_amt / bom_alloc_qty_val
                            if bom_alloc_qty_val > 0 else 0.0
                        ),
                    })
                    store_date_keys.add((key[0], key[1]))

        # ── Step 8: 处理单独parent（未在共享组中）───────────────────────
        solo_parent_count = 0
        for key, parents_dict in parent_subs.items():
            for parent_id, subs in parents_dict.items():
                if parent_id in parent_to_group.get(key, {}):
                    continue  # 已在共享组中

                parent_amt = parent_info[key][parent_id]['amt']
                parent_qty = parent_info[key][parent_id]['qty']
                solo_parent_count += 1

                # 单独parent的Σ总权重
                parent_total_weight = sum(
                    sub_weights.get((key[0], key[1], s), {}).get('weight', 0)
                    for s in subs
                )

                for sub_id in subs:
                    sub_key = (key[0], key[1], sub_id)
                    weight_info = sub_weights.get(sub_key, {})
                    sub_info = sub_lookup.get(sub_key, {})
                    split_need_weight = weight_info.get('weight', 0)

                    if parent_total_weight > 0:
                        alloc_ratio = split_need_weight / parent_total_weight
                        bom_alloc_amt = alloc_ratio * parent_amt
                    else:
                        alloc_ratio = 0
                        bom_alloc_amt = 0

                    # 衍生字段
                    sale_qty_val = sub_info.get('sale_qty', 0)
                    know_lost_qty_val = sub_info.get('know_lost_qty', 0)
                    list_price_val = sub_info.get('list_price', 0)
                    consume_qty_val = sale_qty_val + know_lost_qty_val
                    consume_weight_val = consume_qty_val * list_price_val
                    self_inbound_qty_val = weight_info.get('self_inbound_qty', 0)
                    self_inbound_weight_val = self_inbound_qty_val * list_price_val
                    is_type_a_val = weight_info.get('is_type_a', False)
                    split_need_qty_val = (
                        consume_qty_val - self_inbound_qty_val
                        if is_type_a_val else consume_qty_val
                    )
                    bom_alloc_qty_val = split_need_qty_val

                    results.append({
                        'store_id': key[0],
                        'business_date': key[1],
                        'parent_article_id': parent_id,
                        'sub_article_id': sub_id,
                        'parent_inbound_qty': parent_qty,
                        'parent_inbound_amt': parent_amt,
                        'parent_unit_price': parent_amt / parent_qty if parent_qty > 0 else 0,
                        'sale_qty': sale_qty_val,
                        'know_lost_qty': know_lost_qty_val,
                        'list_price': list_price_val,
                        'consume_qty': consume_qty_val,
                        'consume_weight': consume_weight_val,
                        'self_inbound_qty': self_inbound_qty_val,
                        'self_inbound_amt': weight_info.get('self_inbound_amt', 0),
                        'self_inbound_weight': self_inbound_weight_val,
                        'is_type_a': 1 if is_type_a_val else 0,
                        'split_need_weight': split_need_weight,
                        'split_need_qty': split_need_qty_val,
                        'bom_alloc_qty': bom_alloc_qty_val,
                        'group_total_weight': parent_total_weight,
                        'alloc_ratio': alloc_ratio,
                        'bom_alloc_amt': bom_alloc_amt,
                        'dressing_rate': alloc_ratio,
                        'cost_rate_effective': 0.0,
                        'sub_qty_actual': consume_qty_val,
                        'sub_qty_source': 'SALES+LOSS',
                        'sub_alloc_amt': bom_alloc_amt,
                        'sub_unit_cost': (
                            bom_alloc_amt / bom_alloc_qty_val
                            if bom_alloc_qty_val > 0 else 0.0
                        ),
                    })
                    store_date_keys.add((key[0], key[1]))

        # ── Step 9: 写入结果表 ────────────────────────────────────────
        self._create_empty_table()

        if results:
            insert_sql = f"""
                INSERT INTO {self.TARGET_TABLE} (
                    store_id, business_date, parent_article_id, sub_article_id,
                    parent_inbound_qty, parent_inbound_amount, parent_unit_price,
                    sale_qty, know_lost_qty, consume_qty, consume_weight,
                    self_inbound_qty, self_inbound_amt, self_inbound_weight, is_type_a,
                    split_need_weight, split_need_qty, group_total_weight, alloc_ratio,
                    bom_alloc_amt, bom_alloc_qty,
                    dressing_rate, cost_rate_effective,
                    sub_qty_actual, sub_qty_source,
                    sub_alloc_amt, sub_unit_cost,
                    cost_rate_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            for r in results:
                values = (
                    r['store_id'],
                    r['business_date'],
                    r['parent_article_id'],
                    r['sub_article_id'],
                    r['parent_inbound_qty'],
                    r['parent_inbound_amt'],
                    r['parent_unit_price'],
                    r['sale_qty'],
                    r['know_lost_qty'],
                    r['consume_qty'],
                    r['consume_weight'],
                    r['self_inbound_qty'],
                    r['self_inbound_amt'],
                    r['self_inbound_weight'],
                    r['is_type_a'],
                    r['split_need_weight'],
                    r['split_need_qty'],
                    r['group_total_weight'],
                    r['alloc_ratio'],
                    r['bom_alloc_amt'],
                    r['bom_alloc_qty'],
                    r['dressing_rate'],
                    r['cost_rate_effective'],
                    r['sub_qty_actual'],
                    r['sub_qty_source'],
                    r['sub_alloc_amt'],
                    r['sub_unit_cost'],
                    'V9_WEIGHT_SHARED'
                )
                self._conn.execute(insert_sql, values)

        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_calc_bom_alloc: {rows} rows")
        # Phase 7: 关键节点 log
        unique_stores = len(set(k[0] for k in store_date_keys))
        total_parents = len(parent_subs)
        self._log.info(
            f"BOM alloc summary: {len(results)} rows, "
            f"{unique_stores} stores, "
            f"{len(store_date_keys)} store×date keys, "
            f"{total_parents} total parents, "
            f"{solo_parent_count} solo parents, "
            f"{sum(len(g) for g in shared_groups.values())} shared groups"
        )
        # 验证 sale_qty 修复生效：输出前5个 sale_qty > 0 的样本
        sales_positive = [r for r in results if r['sale_qty'] > 0][:5]
        if sales_positive:
            self._log.info(
                f"sale_qty > 0 samples (fix verified): "
                + " | ".join(
                    f"{r['sub_article_id']}: sale={r['sale_qty']:.1f}"
                    for r in sales_positive
                )
            )
        else:
            self._log.warning("no sub with sale_qty > 0 — sale_qty fix may not be effective for this date range")

    def _create_empty_table(self):
        self._duck.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TARGET_TABLE} (
                store_id                    VARCHAR,
                business_date               VARCHAR,
                parent_article_id           VARCHAR,
                sub_article_id              VARCHAR,
                parent_inbound_qty          DOUBLE,
                parent_inbound_amount       DOUBLE,
                parent_unit_price           DOUBLE,
                sale_qty                    DOUBLE,
                know_lost_qty               DOUBLE,
                consume_qty                 DOUBLE,
                consume_weight              DOUBLE,
                self_inbound_qty            DOUBLE,
                self_inbound_amt            DOUBLE,
                self_inbound_weight         DOUBLE,
                is_type_a                   INTEGER,
                split_need_weight           DOUBLE,
                split_need_qty              DOUBLE,
                group_total_weight          DOUBLE,
                alloc_ratio                 DOUBLE,
                bom_alloc_amt               DOUBLE,
                bom_alloc_qty               DOUBLE,
                dressing_rate               DOUBLE,
                cost_rate_effective         DOUBLE,
                sub_qty_actual              DOUBLE,
                sub_qty_source              VARCHAR,
                sub_alloc_amt               DOUBLE,
                sub_unit_cost               DOUBLE,
                cost_rate_source            VARCHAR
            )
        """)
