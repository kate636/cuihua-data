"""
域⑫ 订验关系域取数器 (v4 预留，executor 暂不调用)

源表: strategy_fm_order_receive_di
目标: DuckDB atomic_order_receive

业务含义:
    dal_store_order_receive_di —— 订购条码 → 验收条码 的数量占比表。
    用于供应链口径：一张订购单到货后可能被拆成多个验收条码，
    v4.0 暂不使用，留到未来供应链口径细化时再启用。

v4 策略:
    - 只建表骨架（空表）
    - executor 暂不调用 extract，后续按需开启
"""

from ._base import BaseExtractor


class OrderReceiveExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_order_receive"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            store_id,
            inc_day                          AS business_date,
            order_article_id,
            receive_article_id,
            CAST(order_qty    AS DOUBLE)     AS order_qty,
            CAST(receive_qty  AS DOUBLE)     AS receive_qty,
            CAST(rate         AS DOUBLE)     AS rate
        FROM strategy_fm_order_receive_di
        WHERE inc_day BETWEEN '{start}' AND '{end}'
          AND store_id IS NOT NULL
        """

    def ensure_empty_skeleton(self) -> None:
        """v4 占位：建空表骨架，供下游安全 JOIN。"""
        if self._duck.table_exists(self.TARGET_TABLE):
            return
        self._duck.execute(f"""
            CREATE TABLE {self.TARGET_TABLE} (
                store_id           VARCHAR,
                business_date      VARCHAR,
                order_article_id   VARCHAR,
                receive_article_id VARCHAR,
                order_qty          DOUBLE,
                receive_qty        DOUBLE,
                rate               DOUBLE
            )
        """)
        self._log.info(f"{self.TARGET_TABLE}: v4 placeholder empty skeleton")
