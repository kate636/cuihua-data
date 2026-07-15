"""
BOM 关系原子层取数器（新增 · P0）

源表: strategy_dim_store_article_bom_relation
     （商分库实际表名，不是 SQL 示例里的 strategy_fm_dim_bom_relation）
目标: DuckDB atomic_bom_relation
粒度: store_id × business_date(=inc_day) × parent_article_id × sub_article_id

业务含义:
    BOM (Bill of Materials) 定义 parent -> sub 的"出肉率 / 成本比例"
    典型场景: 猪肉 — 整头猪 / 白条猪 (parent) 拆成 排骨 / 五花 / 梅头 / 碎油 (sub)

字段语义:
    dressing_rate   标准出肉率(%)  同一 parent 下所有 sub 合计 ≈ 100
    cost_rate       成本比例(%)    可能为 NULL, 下游用 sub 销售原价 × dressing_rate 倒推
    bom_type        1门店bom / 2仓bom / 3非门店非仓bom
    split_mode      10一拆多 / 20一拆一
    sp_level        BI 店铺等级（当前数据全部为 170，不做过滤）

过滤条件:
    store_id / parent_article_id / sub_article_id 均不为 NULL
    bom_type IN (1, 2, 3)

降级策略:
    strategy_dim_store_article_bom_relation 未在商分库落库时 (Unknown table)
    自动降级为空表, 下游 t_calc_bom_cost 也为空, amounts.py 的 COALESCE
    回退到原 cost_price 逻辑, 等价于 v3.0 行为, 不会中断整个 pipeline。
"""

from ._base import BaseExtractor


class BomRelationExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_bom_relation"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            store_id,
            inc_day                                    AS business_date,
            parent_article_id,
            sub_article_id,
            parent_article_unit                        AS parent_unit,
            sub_article_unit                           AS sub_unit,
            CAST(dressing_rate AS DOUBLE)              AS dressing_rate,
            CAST(cost_rate     AS DOUBLE)              AS cost_rate,
            CAST(bom_type      AS INTEGER)             AS bom_type,
            split_mode,
            CAST(sp_level      AS INTEGER)             AS sp_level
        FROM strategy_dim_store_article_bom_relation
        WHERE inc_day BETWEEN '{start}' AND '{end}'
          AND store_id            IS NOT NULL
          AND parent_article_id   IS NOT NULL
          AND sub_article_id      IS NOT NULL
          AND bom_type IN (1, 2, 3)
        """

    def extract(self, start: str, end: str, yesterday: str, chunk: int = 7) -> None:
        """覆盖基类 extract: 捕获 'Unknown table' 错误降级为空表。

        BOM 关系是按日全量快照, 不存在跨日增量诉求; 每次 extract 前 DROP
        旧表, 避免历史降级时建的 schema 和正常 df schema 错位。
        """
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")

        try:
            super().extract(start=start, end=end, yesterday=yesterday, chunk=chunk)
            return
        except RuntimeError as e:
            msg = str(e).lower()
            if "unknown table" in msg and "bom_relation" in msg:
                self._log.warning(
                    "⚠ strategy_dim_store_article_bom_relation 未在商分库落库，"
                    "降级为空表。请先在商分库建表。"
                )
            else:
                raise

        # 降级：落一个空表（带 schema），下游 BomCostCalculator 会正确处理
        self._duck.execute(f"""
            CREATE TABLE {self.TARGET_TABLE} (
                store_id            VARCHAR,
                business_date       VARCHAR,
                parent_article_id   VARCHAR,
                sub_article_id      VARCHAR,
                parent_unit         VARCHAR,
                sub_unit            VARCHAR,
                dressing_rate       DOUBLE,
                cost_rate           DOUBLE,
                bom_type            INTEGER,
                split_mode          VARCHAR,
                sp_level            INTEGER
            )
        """)
        self._log.info(f"{self.TARGET_TABLE}: fallback empty (0 rows)")
