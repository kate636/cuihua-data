"""
域⑪ BOM 拆分事实域取数器 (v4 新增)

源表: strategy_fm_receive_sale_di
目标: DuckDB atomic_receive_sale

粒度:
    (store_id, business_date, article_id=根parent, sale_article_id=销售sub)

业务含义:
    v4 以此表替代 v3.1.x 自造的"BOM 关系递归 + cost_rate 倒推"方案。
    上游已经把"当天门店从每个根 parent 拆出多少 qty / 多少钱" 算好了，
    这张表是最接近"BOM 当天实际动作"的原子事实，粒度精到 (parent, sub)。

字段语义（全量保留 20 列）:
    article_id              -- 父（根 parent, 如"优鲜黑猪A级"）
    article_name
    sale_article_id         -- 子（销售 sku, 如"优鲜黑猪梅头肉"）
    sale_article_name
    inbound_qty             -- 父当天入库量（kg 或件）
    inbound_amount          -- 父当天入库额（元）
    purchase_price          -- 父入库均价 = inbound_amount / inbound_qty
    sum_article_qty         -- 父入库量（去重）
    sum_sub_article_qty     -- 父下所有 sub 总 qty
    sum_sale_article_qty    -- 该 sub 今天从所有 parent 拆出的总 qty
    sale_article_qty        -- 该 sub 从本 parent 拆出的 qty
    sale_article_price      -- 该 sub 进货分摊单价
    spilit_sale_article_amt -- 该 sub 从本 parent 拆出的金额（核心 BOM 分摊结果）
    rate                    -- 该 parent 给 sub 的 qty 占比（多 parent 时按此拆销售）
    sale_recev_rate         -- sub_qty / parent_inbound_qty
    category_level1_id
    category_level1_description
    business_date, inc_day, store_id

注意:
    article_id = sale_article_id 的行表示"标品/非 BOM 拆分"场景，
    此时 rate ≈ 1.0, sale_article_qty ≈ inbound_qty, spilit ≈ inbound_amount。
    v4 下游 t_calc_bom_alloc 对两种情况统一处理。
"""

from ._base import BaseExtractor


class ReceiveSaleExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_receive_sale"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            store_id,
            inc_day                                  AS business_date,
            article_id,
            article_name,
            sale_article_id,
            sale_article_name,
            CAST(inbound_qty              AS DOUBLE) AS inbound_qty,
            CAST(inbound_amount           AS DOUBLE) AS inbound_amount,
            CAST(purchase_price           AS DOUBLE) AS purchase_price,
            CAST(sum_article_qty          AS DOUBLE) AS sum_article_qty,
            CAST(sum_sub_article_qty      AS DOUBLE) AS sum_sub_article_qty,
            CAST(sum_sale_article_qty     AS DOUBLE) AS sum_sale_article_qty,
            CAST(sale_article_qty         AS DOUBLE) AS sale_article_qty,
            CAST(sale_article_price       AS DOUBLE) AS sale_article_price,
            CAST(spilit_sale_article_amt  AS DOUBLE) AS split_sale_article_amt,
            CAST(rate                     AS DOUBLE) AS rate,
            CAST(sale_recev_rate          AS DOUBLE) AS sale_recev_rate,
            category_level1_id,
            category_level1_description
        FROM strategy_fm_receive_sale_di
        WHERE inc_day BETWEEN '{start}' AND '{end}'
          AND store_id         IS NOT NULL
          AND article_id       IS NOT NULL
          AND sale_article_id  IS NOT NULL
        """

    def extract(self, start: str, end: str, yesterday: str, chunk: int = 7) -> None:
        """分区写入模式，保留历史数据。"""
        try:
            super().extract(start=start, end=end, yesterday=yesterday, chunk=chunk)
            return
        except RuntimeError as e:
            msg = str(e).lower()
            if "unknown table" in msg and "receive_sale" in msg:
                self._log.warning(
                    "⚠ strategy_fm_receive_sale_di 未在商分库落库，降级为空表。"
                )
            else:
                raise

        self._duck.execute(f"""
            CREATE TABLE {self.TARGET_TABLE} (
                store_id                    VARCHAR,
                business_date               VARCHAR,
                article_id                  VARCHAR,
                article_name                VARCHAR,
                sale_article_id             VARCHAR,
                sale_article_name           VARCHAR,
                inbound_qty                 DOUBLE,
                inbound_amount              DOUBLE,
                purchase_price              DOUBLE,
                sum_article_qty             DOUBLE,
                sum_sub_article_qty         DOUBLE,
                sum_sale_article_qty        DOUBLE,
                sale_article_qty            DOUBLE,
                sale_article_price          DOUBLE,
                split_sale_article_amt      DOUBLE,
                rate                        DOUBLE,
                sale_recev_rate             DOUBLE,
                category_level1_id          VARCHAR,
                category_level1_description VARCHAR
            )
        """)
        self._log.info(f"{self.TARGET_TABLE}: fallback empty (0 rows)")
