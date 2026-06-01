"""
域⑬ 单位互转域取数器 (v4 预留，executor 暂不调用)

源表: strategy_fm_dim_article_convert
目标: DuckDB atomic_article_convert (dim_store_article_convert_info_da 全量快照)

业务含义:
    门店 × 商品 的单位互转比率（件 ↔ kg, 箱 ↔ 盒 等）。
    v4.0 单位换算仍走 dim_goods.unit_weight，
    这张表留给未来"规格变体/多单位精细化"使用。
"""

from ._base import BaseExtractor


class ArticleConvertExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_article_convert"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            store_id,
            inc_day                       AS business_date,
            article_id,
            src_unit,
            dest_unit,
            CAST(convert_rate AS DOUBLE)  AS convert_rate
        FROM strategy_fm_dim_article_convert
        WHERE inc_day BETWEEN '{start}' AND '{end}'
          AND store_id IS NOT NULL
        """

    def ensure_empty_skeleton(self) -> None:
        """v4 占位：建空表骨架，供下游安全 JOIN。"""
        if self._duck.table_exists(self.TARGET_TABLE):
            return
        self._duck.execute(f"""
            CREATE TABLE {self.TARGET_TABLE} (
                store_id       VARCHAR,
                business_date  VARCHAR,
                article_id     VARCHAR,
                src_unit       VARCHAR,
                dest_unit      VARCHAR,
                convert_rate   DOUBLE
            )
        """)
        self._log.info(f"{self.TARGET_TABLE}: v4 placeholder empty skeleton")
