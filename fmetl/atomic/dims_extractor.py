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
        self._extract_goods(end)  # v0.10 fix: 用end确保dim_goods是最新日期
        self._build_day_clear_override()  # v0.10 fix: 从dim_goods派生日清覆盖白名单
        self._extract_store_list(start, end)
        self._extract_day_clear(start, end)
        self._extract_store_profile(yesterday)
        self._extract_calendar(start, end)
        self._extract_chdj_store_info()
        self._extract_saleable(yesterday)

    def _extract_goods(self, yesterday: str) -> None:
        # strategy_fm_dim_goods是日快照表，有incDay字段
        # 以end日期为基准，前后各扫描几天，取最近的有数据快照
        from datetime import date as dt_date, timedelta
        df = None
        # 优先查 end 到 end-7 天，再查 end+3 到 end（覆盖今天同步但ETL跑历史日期的情况）
        base_date = dt_date.fromisoformat(yesterday)
        scan_dates = [base_date - timedelta(days=i) for i in range(8)]  # yesterday back 7 days
        scan_dates += [base_date + timedelta(days=i) for i in range(1, 4)]  # forward up to 3 days
        for try_date in scan_dates:
            ds = try_date.isoformat()
            sql = f"""
            SELECT
                article_id, article_name,
                category_level1_id, category_level1_description,
                category_level2_id, category_level2_description,
                category_level3_id, category_level3_description,
                spu_id, spu_name,
                blackwhite_pig_name, blackwhite_pig_id,
                unit_weight, sale_unit
            FROM strategy_fm_dim_goods
            WHERE inc_day = '{ds}'
              AND category_level1_id NOT IN ('70','71','72','73','74','75','76','77')
            """
            df = self._sr.query(sql)
            if not df.empty:
                self._log.info(f"dim_goods found at inc_day={ds}")
                break
        if df is not None and not df.empty:
            self._duck.load_df(df, "dim_goods", mode="replace")
            self._log.info(f"dim_goods: {len(df)} rows")
        else:
            # 无新数据时保留现有 dim_goods 不覆盖
            existing = self._duck.row_count("dim_goods")
            self._log.warning(f"dim_goods: no data in last 7 days, keeping existing ({existing} rows)")

    def _build_day_clear_override(self) -> None:
        """基于 dim_goods 构建日清覆盖白名单（仅 article_id），供 merge.py 使用。
        将 merge.py 对 dim_goods 的依赖隔离到此辅助表中。

        烘焙类清单从 FM 经营平台 API 拉取（业务自行维护），
        猪肉类/熟食类按分类规则自动覆盖。"""
        import requests

        BAKERY_FALLBACK = [
            '21333774','21333798','21334108','21334115','21334146','21334153','21334160',
            '21334177','21334184','21334191','21334207','21334221','21336645','21346026',
            '21346033','21346040','21346057','21346064','21346583','21346590','21346705',
            '21346729','21346736','21346743',
        ]

        # 从日清品清单 API 拉取烘焙类 SKU
        bakery_skus = BAKERY_FALLBACK
        try:
            resp = requests.get(
                "http://127.0.0.1:5006/api/dayclear/list",
                params={"type": "烘焙类", "active_only": "1"},
                timeout=5,
            )
            if resp.ok:
                items = resp.json()
                if items:
                    bakery_skus = [item["article_id"] for item in items]
                    self._log.info(f"dayclear API: {len(bakery_skus)} bakery SKUs fetched")
            else:
                self._log.warning(f"dayclear API returned {resp.status_code}, using fallback")
        except Exception as e:
            self._log.warning(f"dayclear API unavailable ({e}), using fallback list")

        try:
            self._duck.execute("DROP TABLE IF EXISTS dim_day_clear_override")
            bakery_list = "', '".join(bakery_skus)
            self._duck.execute(f"""
                CREATE TABLE dim_day_clear_override AS
                SELECT DISTINCT article_id, '猪肉类' AS override_type
                FROM dim_goods WHERE category_level1_description = '猪肉类'
                UNION ALL
                SELECT DISTINCT article_id, '熟食类' AS override_type
                FROM dim_goods WHERE category_level3_description LIKE '%熟食'
                UNION ALL
                SELECT DISTINCT article_id, '烘焙类' AS override_type
                FROM dim_goods WHERE article_id IN ('{bakery_list}')
            """)
            cnt = self._duck.row_count("dim_day_clear_override")
            self._log.info(f"dim_day_clear_override built: {cnt} SKUs (bakery: {len(bakery_skus)})")
        except Exception as e:
            self._log.warning(f"dim_day_clear_override build skipped: {e}")

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
        from ..utils.date_utils import split_date_range
        total = 0
        for seg_start, seg_end in split_date_range(start, end, 7):
            sql = f"""
            SELECT DISTINCT
                business_date,
                store_id,
                article_id,
                1 AS day_clear
            FROM strategy_fm_dim_day_clear
            WHERE business_date BETWEEN '{seg_start}' AND '{seg_end}'
            """
            df = self._sr.query(sql)
            if df.empty:
                continue
            self._duck.load_df(df, "dim_day_clear", mode="replace_partition",
                               date_col="business_date", start=seg_start, end=seg_end)
            total += len(df)
        self._log.info(f"dim_day_clear: {total} rows")

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
