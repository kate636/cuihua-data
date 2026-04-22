"""
维度表提取器

一次性提取所有需要的维度表到 DuckDB：
  - dim_goods:           商品主数据               ← strategy_fm_dim_goods
  - dim_store_list:      翠花门店白名单           ← default_catalog.ads_business_analysis.chdj_store_info
                         TODO: 等 strategy_fm_dim_store_list 建成后切回
  - dim_day_clear:       日清标签                 ← strategy_fm_dim_day_clear
                         WARN: 新表可能包含全量 SKU（92K 行），需要 Phase D 对比验证
  - dim_store_profile:   门店维度(管理区/大区/城市) ← strategy_fm_dim_store_profile
  - dim_calendar:        日历维度                 ← strategy_fm_dim_calendar
  - dim_chdj_store_info: 翠花门店信息(store_flag/store_no) ← default_catalog.ads_business_analysis.chdj_store_info
  - dim_saleable:        可售标识                 ← strategy_fm_dim_saleable
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

    def _extract_goods(self, yesterday: str) -> None:
        sql = """
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
        FROM strategy_fm_dim_goods
        WHERE category_level1_id NOT IN ('70','71','72','73','74','75','76','77')
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_goods", mode="replace")
        self._log.info(f"dim_goods: {len(df)} rows")

    def _extract_store_list(self, start: str, end: str) -> None:
        # TODO: 等 strategy_fm_dim_store_list 建成后切换为 strategy 表
        sql = """
        SELECT DISTINCT store_id
        FROM default_catalog.ads_business_analysis.chdj_store_info
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_store_list", mode="replace")
        self._log.info(f"dim_store_list: {len(df)} rows")

    def _extract_day_clear(self, start: str, end: str) -> None:
        # WARN: strategy_fm_dim_day_clear 可能包含全量 SKU（~92K 行），与原
        # dim_day_clear_article_list_di "仅日清 SKU" 的语义不一致。Phase D 跑完后
        # 需对比 `t_atomic_wide.day_clear='1'` 比例；若异常请切回 category 过滤兜底。
        sql = f"""
        SELECT DISTINCT
            business_date,
            store_id,
            article_id,
            1 AS day_clear
        FROM strategy_fm_dim_day_clear
        WHERE business_date BETWEEN '{start}' AND '{end}'
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_day_clear", mode="replace_partition",
                           date_col="business_date", start=start, end=end)
        self._log.info(f"dim_day_clear: {len(df)} rows")

    def _extract_store_profile(self, yesterday: str) -> None:
        sql = """
        SELECT
            sp_store_id     AS store_id,
            sp_store_name   AS store_name,
            manage_area_name,
            sap_area_name,
            city_description
        FROM strategy_fm_dim_store_profile
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_store_profile", mode="replace")
        self._log.info(f"dim_store_profile: {len(df)} rows")

    def _extract_calendar(self, start: str, end: str) -> None:
        sql = f"""
        SELECT
            day_date        AS business_date,
            week_no,
            week_start_date,
            week_end_date,
            month_wid,
            year_wid
        FROM strategy_fm_dim_calendar
        WHERE day_date BETWEEN '{start}' AND '{end}'
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_calendar", mode="replace")
        self._log.info(f"dim_calendar: {len(df)} rows")

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

    def _extract_saleable(self, yesterday: str) -> None:
        sql = """
        SELECT DISTINCT
            shop_id     AS store_id,
            sku_code    AS article_id
        FROM strategy_fm_dim_saleable
        """
        df = self._sr.query(sql)
        self._duck.load_df(df, "dim_saleable", mode="replace")
        self._log.info(f"dim_saleable: {len(df)} rows")
