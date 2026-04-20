"""
维度表提取器

一次性提取所有需要的维度表到 DuckDB：
  - dim_goods:       商品主数据
  - dim_store_list:  翠花门店列表
  - dim_day_clear:   日清标签
  - dim_store_profile: 门店维度(管理区/大区/城市)
  - dim_calendar:    日历维度
  - dim_chdj_store_info: 翠花门店信息(store_flag/store_no)
  - dim_saleable:    可订可售标识
"""

from __future__ import annotations

from ..connectors import ApiConnector, DuckDBStore
from ..utils import get_logger


class DimsExtractor:
    """维度表提取器（非分批，一次性加载最新快照）。"""

    def __init__(self, sr: ApiConnector, duck: DuckDBStore):
        self._sr = sr
        self._duck = duck
        self._log = get_logger("DimsExtractor")

    def extract_all(self, yesterday: str, start: str, end: str) -> None:
        """提取所有维度表快照。"""
        self._extract_goods(yesterday)
        self._extract_store_list(start, end)
        self._extract_day_clear(start, end)
        self._extract_store_profile(yesterday)
        self._extract_calendar(start, end)
        self._extract_chdj_store_info()
        self._extract_saleable(yesterday)

    # ── 商品主数据 ─────────────────────────────────────────────────────────────
    def _extract_goods(self, yesterday: str) -> None:
        sql = f"""
        SELECT
            article_id,
            article_name,
            category_level1_id,
            category_level1_description,
            category_level2_id,
            category_level2_description,
            category_level3_id,
            category_level3_description,
            spu_id,
            spu_name,
            blackwhite_pig_name,
            blackwhite_pig_id,
            unit_weight,
            sale_unit
        FROM hive.dim.dim_goods_information_have_pt
        WHERE inc_day = '{yesterday}'
          AND category_level1_id NOT IN ('70','71','72','73','74','75','76','77')
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_goods", mode="replace")
        self._log.info(f"dim_goods: {len(df)} rows")

    # ── 翠花门店列表 ──────────────────────────────────────────────────────────
    def _extract_store_list(self, start: str, end: str) -> None:
        sql = f"""
        SELECT DISTINCT store_id
        FROM hive.dim.dim_chdj_store_list_di
        WHERE inc_day BETWEEN '{start}' AND '{end}'
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_store_list", mode="replace")
        self._log.info(f"dim_store_list: {len(df)} rows")

    # ── 日清标签 ──────────────────────────────────────────────────────────────
    def _extract_day_clear(self, start: str, end: str) -> None:
        sql = f"""
        SELECT DISTINCT
            business_date,
            store_id,
            article_id,
            1 AS day_clear
        FROM hive.dim.dim_day_clear_article_list_di
        WHERE inc_day BETWEEN '{start}' AND '{end}'
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_day_clear", mode="replace_partition",
                           date_col="business_date", start=start, end=end)
        self._log.info(f"dim_day_clear: {len(df)} rows")

    # ── 门店维度 ──────────────────────────────────────────────────────────────
    def _extract_store_profile(self, yesterday: str) -> None:
        sql = f"""
        SELECT
            sp_store_id     AS store_id,
            sp_store_name   AS store_name,
            manage_area_name,
            sap_area_name,
            city_description
        FROM hive.dim.dim_store_profile
        WHERE inc_day = '{yesterday}'
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_store_profile", mode="replace")
        self._log.info(f"dim_store_profile: {len(df)} rows")

    # ── 日历维度 ──────────────────────────────────────────────────────────────
    def _extract_calendar(self, start: str, end: str) -> None:
        sql = f"""
        SELECT
            date_key        AS business_date,
            week_no,
            week_start_date,
            week_end_date,
            month_wid,
            year_wid
        FROM hive.dim.dim_calendar
        WHERE date_key BETWEEN '{start}' AND '{end}'
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_calendar", mode="replace")
        self._log.info(f"dim_calendar: {len(df)} rows")

    # ── 翠花门店信息 ──────────────────────────────────────────────────────────
    def _extract_chdj_store_info(self) -> None:
        sql = """
        SELECT
            store_id,
            store_flag,
            store_no,
            store_name
        FROM default_catalog.ads_business_analysis.chdj_store_info
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_chdj_store_info", mode="replace")
        self._log.info(f"dim_chdj_store_info: {len(df)} rows")

    # ── 可订可售标识 ──────────────────────────────────────────────────────────
    def _extract_saleable(self, yesterday: str) -> None:
        sql = f"""
        SELECT DISTINCT
            shop_id     AS store_id,
            article_id
        FROM hive.ods_sc_db.t_purchase_order_item_tmp
        WHERE inc_day = '{yesterday}'
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_saleable", mode="replace")
        self._log.info(f"dim_saleable: {len(df)} rows")
