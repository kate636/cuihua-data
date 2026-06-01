"""
t_calc_bom_alloc — BOM 分摊事实表 (v10)

核心逻辑:
  1. 消耗权重 = (sale_qty + know_lost_qty) × list_price
  2. 自己进货权重 = self_inbound_qty × list_price
  3. 拆分需求权重 = 消耗权重 - 自己进货权重 (Type A) 或 消耗权重 (Type B/C)
  4. parent共享组识别（子集关系合并）
  5. 分配占比 = 拆分需求权重 / Σ总权重
  6. bom_alloc_amt = 分配占比 × 组总进货额

v10 修复:
  A13: 共享组 parent_inbound_qty 取组总 qty，非第一个 parent
  A14: split_need_qty = max(0, consume_qty - self_inbound_qty) 防负
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger


class BomAllocCalculator:
    TARGET_TABLE = "t_calc_bom_alloc"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("BomAllocCalculator")
        self._conn = duck._conn

    def run(self) -> None:
        self._log.info("calculating BOM allocation (v10) ...")

        # Step 1: BOM关系（只取 parent != sub 的行）
        bom_relations = self._conn.execute("""
            SELECT
                store_id,
                business_date,
                article_id                          AS parent_article_id,
                sale_article_id                     AS sub_article_id,
                inbound_qty                         AS parent_inbound_qty,
                inbound_amount                      AS parent_inbound_amount,
                sum_sub_article_qty                 AS parent_sum_sub_qty
            FROM atomic_receive_sale
            WHERE article_id != sale_article_id
              AND store_id IS NOT NULL
              AND article_id IS NOT NULL
              AND sale_article_id IS NOT NULL
        """).fetchall()

        if not bom_relations:
            self._log.warning("no BOM relation data, creating empty table")
            self._create_empty_table()
            return

        # Step 2: 构建 parent→subs 映射
        parent_subs = {}
        parent_info = {}

        for row in bom_relations:
            store_id, date, parent_id, sub_id, qty, amt, sum_sub_qty = row
            key = (store_id, date)
            if key not in parent_subs:
                parent_subs[key] = {}
                parent_info[key] = {}

            if parent_id not in parent_subs[key]:
                parent_subs[key][parent_id] = set()
                parent_info[key][parent_id] = {
                    'amt': None, 'qty': None, 'sum_sub_qty': None}

            parent_subs[key][parent_id].add(sub_id)
            if parent_info[key][parent_id]['amt'] is None:
                parent_info[key][parent_id]['amt'] = amt if amt else 0
                parent_info[key][parent_id]['qty'] = qty if qty else 0
                parent_info[key][parent_id]['sum_sub_qty'] = sum_sub_qty if sum_sub_qty else qty

        # Step 3: 识别parent共享组
        shared_groups = {}
        parent_to_group = {}

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

                    if p2_subs.issubset(p1_subs):
                        self._log.info(f"shared group: {p2}.subs ⊆ {p1}.subs")
                        group = {
                            'parents': [p1, p2],
                            'subs': p1_subs.union(p2_subs),
                            'total_amt': (parent_info[key][p1]['amt']
                                          + parent_info[key][p2]['amt']),
                            'total_qty': (parent_info[key][p1]['qty']
                                          + parent_info[key][p2]['qty']),
                            'p_amt': {p1: parent_info[key][p1]['amt'],
                                       p2: parent_info[key][p2]['amt']},
                            'p_qty': {p1: parent_info[key][p1]['qty'],
                                       p2: parent_info[key][p2]['qty']},
                            'p_sum_sub_qty': {p1: parent_info[key][p1]['sum_sub_qty'],
                                               p2: parent_info[key][p2]['sum_sub_qty']},
                            'p_subs': {p1: p1_subs, p2: p2_subs},
                        }
                        shared_groups[key].append(group)
                        parent_to_group[key][p1] = len(shared_groups[key]) - 1
                        parent_to_group[key][p2] = len(shared_groups[key]) - 1

                    elif p1_subs.issubset(p2_subs):
                        self._log.info(f"shared group: {p1}.subs ⊆ {p2}.subs")
                        group = {
                            'parents': [p2, p1],
                            'subs': p1_subs.union(p2_subs),
                            'total_amt': (parent_info[key][p2]['amt']
                                          + parent_info[key][p1]['amt']),
                            'total_qty': (parent_info[key][p2]['qty']
                                          + parent_info[key][p1]['qty']),
                            'p_amt': {p2: parent_info[key][p2]['amt'],
                                       p1: parent_info[key][p1]['amt']},
                            'p_qty': {p2: parent_info[key][p2]['qty'],
                                       p1: parent_info[key][p1]['qty']},
                            'p_sum_sub_qty': {p2: parent_info[key][p2]['sum_sub_qty'],
                                               p1: parent_info[key][p1]['sum_sub_qty']},
                            'p_subs': {p2: p2_subs, p1: p1_subs},
                        }
                        shared_groups[key].append(group)
                        parent_to_group[key][p1] = len(shared_groups[key]) - 1
                        parent_to_group[key][p2] = len(shared_groups[key]) - 1

        # Step 4: 获取 sub 数据
        sub_data = self._conn.execute("""
            SELECT
                s.store_id,
                s.business_date,
                s.article_id                        AS sub_article_id,
                COALESCE(s.sale_qty, 0)             AS sale_qty,
                COALESCE(p.original_price, 0)       AS list_price,
                COALESCE(l.know_lost_qty, 0)        AS know_lost_qty
            FROM (SELECT store_id, business_date, article_id,
                         SUM(sale_qty) AS sale_qty
                  FROM atomic_sales
                  GROUP BY store_id, business_date, article_id) s
            LEFT JOIN (SELECT store_id, business_date, article_id,
                              SUM(know_lost_qty) AS know_lost_qty
                       FROM atomic_loss
                       GROUP BY store_id, business_date, article_id) l
              ON l.store_id = s.store_id
             AND l.business_date = s.business_date
             AND l.article_id = s.article_id
            LEFT JOIN (SELECT store_id, business_date, article_id,
                              AVG(original_price) AS original_price
                       FROM atomic_price
                       GROUP BY store_id, business_date, article_id) p
              ON p.store_id = s.store_id
             AND p.business_date = s.business_date
             AND p.article_id = s.article_id
        """).fetchall()

        sub_lookup = {}
        for row in sub_data:
            store, date, sub_id, sale_qty, list_price, know_lost_qty = row
            sub_lookup[(store, date, sub_id)] = {
                'sale_qty': sale_qty,
                'list_price': list_price,
                'know_lost_qty': know_lost_qty
            }

        # Step 5: 获取自己进货数据
        self_inbound = self._conn.execute("""
            SELECT
                store_id,
                business_date,
                article_id,
                SUM(inbound_qty)    AS self_inbound_qty,
                SUM(inbound_amount) AS self_inbound_amt
            FROM atomic_receive_sale
            WHERE article_id = sale_article_id
            GROUP BY store_id, business_date, article_id
        """).fetchall()

        self_inbound_lookup = {}
        for row in self_inbound:
            store, date, article_id, qty, amt = row
            self_inbound_lookup[(store, date, article_id)] = {'qty': qty, 'amt': amt}

        # Step 6: 计算每个 sub 的拆分需求权重
        sub_weights = {}
        for key, parents_dict in parent_subs.items():
            all_subs = set()
            for parent_id, subs in parents_dict.items():
                all_subs.update(subs)

            for sub_id in all_subs:
                sub_key = (key[0], key[1], sub_id)
                sub_info = sub_lookup.get(sub_key,
                    {'sale_qty': 0, 'list_price': 0, 'know_lost_qty': 0})
                self_info = self_inbound_lookup.get(sub_key,
                    {'qty': 0, 'amt': 0})

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

        # Step 7: 计算分配金额
        store_date_keys = set()
        results = []

        # 7a: 共享组 — v9逻辑: 所有子品(含独有)用组总额做基数
        for key, groups in shared_groups.items():
            for group in groups:
                group_subs = group['subs']
                group_total_amt = group['total_amt']
                group_total_qty = group['total_qty']

                group_total_weight = sum(
                    sub_weights.get((key[0], key[1], s), {}).get('weight', 0)
                    for s in group_subs
                )

                p0, p1 = group['parents'][0], group['parents'][1]
                p0_amt_ratio = group['p_amt'][p0] / group_total_amt if group_total_amt > 0 else 1.0
                p1_amt_ratio = group['p_amt'][p1] / group_total_amt if group_total_amt > 0 else 0.0

                for sub_id in group_subs:
                    sub_key = (key[0], key[1], sub_id)
                    weight_info = sub_weights.get(sub_key, {})
                    sub_info = sub_lookup.get(sub_key, {})
                    split_need_weight = weight_info.get('weight', 0)

                    alloc_ratio = (split_need_weight / group_total_weight
                                   if group_total_weight > 0 else 0)
                    bom_alloc_amt = alloc_ratio * group_total_amt

                    in_p0 = sub_id in group['p_subs'][p0]
                    in_p1 = sub_id in group['p_subs'][p1]

                    # 所有子品(独有+共享): 按进货额比例拆分amt+qty给两个父品
                    # 保证每个父品bom_out = receive_amt
                    for p, p_ratio in [(p0, p0_amt_ratio), (p1, p1_amt_ratio)]:
                        results.append(
                            self._build_result_row(
                                key, sub_id, sub_key, weight_info, sub_info,
                                p,
                                group['p_amt'][p],
                                group['p_qty'][p],
                                alloc_ratio * p_ratio,
                                bom_alloc_amt * p_ratio,
                                group_total_weight,
                                parent_sum_sub_qty=group['p_sum_sub_qty'][p],
                                qty_split_ratio=p_ratio,
                            ))
                    store_date_keys.add((key[0], key[1]))

        # 7b: 单独 parent
        solo_parent_count = 0
        for key, parents_dict in parent_subs.items():
            for parent_id, subs in parents_dict.items():
                if parent_id in parent_to_group.get(key, {}):
                    continue

                parent_amt = parent_info[key][parent_id]['amt']
                parent_qty = parent_info[key][parent_id]['qty']
                parent_sum_sub_qty = parent_info[key][parent_id]['sum_sub_qty']
                solo_parent_count += 1

                parent_total_weight = sum(
                    sub_weights.get((key[0], key[1], s), {}).get('weight', 0)
                    for s in subs
                )

                for sub_id in subs:
                    sub_key = (key[0], key[1], sub_id)
                    weight_info = sub_weights.get(sub_key, {})
                    sub_info = sub_lookup.get(sub_key, {})
                    split_need_weight = weight_info.get('weight', 0)

                    alloc_ratio = (split_need_weight / parent_total_weight
                                   if parent_total_weight > 0 else 0)
                    bom_alloc_amt = alloc_ratio * parent_amt

                    results.append(
                        self._build_result_row(
                            key, sub_id, sub_key, weight_info, sub_info,
                            parent_id, parent_amt, parent_qty,
                            alloc_ratio, bom_alloc_amt, parent_total_weight,
                            parent_sum_sub_qty=parent_sum_sub_qty,
                        ))
                    store_date_keys.add((key[0], key[1]))

        # Step 8: 写入结果
        self._create_empty_table()

        if results:
            # 删除本次要写入的日期范围（分区覆盖）
            dates = sorted(set(r['business_date'] for r in results))
            if dates:
                self._conn.execute(
                    f"DELETE FROM {self.TARGET_TABLE} "
                    f"WHERE business_date BETWEEN '{dates[0]}' AND '{dates[-1]}'"
                )
            insert_sql = f"""
                INSERT INTO {self.TARGET_TABLE} (
                    store_id, business_date, parent_article_id, sub_article_id,
                    parent_inbound_qty, parent_inbound_amount, parent_unit_price,
                    sale_qty, know_lost_qty, consume_qty, consume_weight,
                    self_inbound_qty, self_inbound_amt, self_inbound_weight, is_type_a,
                    split_need_weight, split_need_qty, group_total_weight, alloc_ratio,
                    bom_alloc_amt, bom_alloc_qty, bom_alloc_qty_sub,
                    dressing_rate, cost_rate_effective, cost_rate_source,
                    sub_qty_actual, sub_qty_source,
                    sub_alloc_amt, sub_unit_cost
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            for r in results:
                self._conn.execute(insert_sql, (
                    r['store_id'], r['business_date'],
                    r['parent_article_id'], r['sub_article_id'],
                    r['parent_inbound_qty'], r['parent_inbound_amount'],
                    r['parent_unit_price'],
                    r['sale_qty'], r['know_lost_qty'],
                    r['consume_qty'], r['consume_weight'],
                    r['self_inbound_qty'], r['self_inbound_amt'],
                    r['self_inbound_weight'], r['is_type_a'],
                    r['split_need_weight'], r['split_need_qty'],
                    r['group_total_weight'], r['alloc_ratio'],
                    r['bom_alloc_amt'], r['bom_alloc_qty'], r['bom_alloc_qty_sub'],
                    r['dressing_rate'], r['cost_rate_effective'],
                    r['cost_rate_source'],
                    r['sub_qty_actual'], r['sub_qty_source'],
                    r['sub_alloc_amt'], r['sub_unit_cost'],
                ))

        rows = self._duck.row_count(self.TARGET_TABLE)
        unique_stores = len(set(k[0] for k in store_date_keys))
        self._log.info(
            f"t_calc_bom_alloc: {rows} rows, {unique_stores} stores, "
            f"{solo_parent_count} solo parents, "
            f"{sum(len(g) for g in shared_groups.values())} shared groups"
        )

    def _build_result_row(self, key, sub_id, sub_key, weight_info, sub_info,
                          parent_id, parent_amt, parent_qty,
                          alloc_ratio, bom_alloc_amt, group_total_weight,
                          parent_sum_sub_qty=None, qty_split_ratio=1.0):
        sale_qty_val = sub_info.get('sale_qty', 0)
        know_lost_qty_val = sub_info.get('know_lost_qty', 0)
        list_price_val = sub_info.get('list_price', 0)
        consume_qty_val = sale_qty_val + know_lost_qty_val
        consume_weight_val = consume_qty_val * list_price_val
        self_inbound_qty_val = weight_info.get('self_inbound_qty', 0)
        self_inbound_weight_val = self_inbound_qty_val * list_price_val
        is_type_a_val = weight_info.get('is_type_a', False)

        # v10 A14 fix: max(0, ...) 防负值
        if is_type_a_val:
            split_need_qty_val = max(0.0, consume_qty_val - self_inbound_qty_val)
            split_need_weight = max(0.0, consume_weight_val - self_inbound_weight_val)
        else:
            split_need_qty_val = consume_qty_val
            split_need_weight = consume_weight_val

        # v10 fix: 按总产量(非销量)分配母品成本到子品
        # alloc_ratio 已编码成本分配权重，qty 同比例从总产量分配
        sum_sub = parent_sum_sub_qty if parent_sum_sub_qty else parent_qty
        bom_alloc_qty_parent = alloc_ratio * parent_qty
        bom_alloc_qty_sub = alloc_ratio * sum_sub

        # 共享组: parent_inbound 显示本父品的单独进货额，非组总额
        return {
            'store_id': key[0],
            'business_date': key[1],
            'parent_article_id': parent_id,
            'sub_article_id': sub_id,
            'parent_inbound_qty': parent_qty,
            'parent_inbound_amount': parent_amt,
            'parent_unit_price': (parent_amt / parent_qty if parent_qty > 0 else 0),
            'sale_qty': sale_qty_val,
            'know_lost_qty': know_lost_qty_val,
            'consume_qty': consume_qty_val,
            'consume_weight': consume_weight_val,
            'self_inbound_qty': self_inbound_qty_val,
            'self_inbound_amt': weight_info.get('self_inbound_amt', 0),
            'self_inbound_weight': self_inbound_weight_val,
            'is_type_a': 1 if is_type_a_val else 0,
            'split_need_weight': split_need_weight,
            'split_need_qty': split_need_qty_val,
            'bom_alloc_qty': bom_alloc_qty_parent,
            'bom_alloc_qty_sub': bom_alloc_qty_sub,
            'group_total_weight': group_total_weight,
            'alloc_ratio': alloc_ratio,
            'bom_alloc_amt': bom_alloc_amt,
            'dressing_rate': alloc_ratio,
            'cost_rate_effective': 0.0,
            'cost_rate_source': 'V10_WEIGHTED_ALLOC',
            'sub_qty_actual': consume_qty_val,
            'sub_qty_source': 'SALES+LOSS',
            'sub_alloc_amt': bom_alloc_amt,
            'sub_unit_cost': (bom_alloc_amt / bom_alloc_qty_sub
                              if bom_alloc_qty_sub > 0 else 0.0),
        }

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
                bom_alloc_qty_sub           DOUBLE,
                dressing_rate               DOUBLE,
                cost_rate_effective         DOUBLE,
                cost_rate_source            VARCHAR,
                sub_qty_actual              DOUBLE,
                sub_qty_source              VARCHAR,
                sub_alloc_amt               DOUBLE,
                sub_unit_cost               DOUBLE,
            )
        """)
