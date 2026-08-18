from __future__ import annotations

from fmetl.contracts import MirrorAuthority, MirrorContract, PartitionMode


# Exact StarRocks targets maintained by v1_5/sync_strategy_fm.sh.
SYNC_MIRROR_TABLES: tuple[str, ...] = (
    "strategy_fm_sales_di",
    "strategy_fm_purchase_di",
    "strategy_fm_scm_di",
    "strategy_fm_scm_adjust_di",
    "strategy_fm_loss_di",
    "strategy_fm_compose_di",
    "strategy_fm_allowance_di",
    "strategy_fm_promo_di",
    "strategy_fm_inventory_pool_di",
    "strategy_fm_price_da",
    "strategy_fm_dim_day_clear",
    "strategy_fm_dim_store_profile",
    "strategy_fm_purchase_order_tmp",
    "strategy_fm_dim_goods",
    "strategy_fm_receive_sale_di",
    "strategy_fm_order_receive_di",
    "strategy_fm_dim_article_convert",
    "strategy_dim_store_article_bom_relation",
    "strategy_fm_store_article_inventory_detail_di",
    "strategy_fm_full_link_article_di",
    "strategy_fm_store_daily_di",
    "strategy_fm_article_sale_di",
    "strategy_fm_chdj_article_di",
    "strategy_fm_dim_order_saleable",
    "strategy_fm_dim_calendar",
    "strategy_fm_order_offline_di",
    "strategy_fm_order_online_di",
    "strategy_fm_trade_user",
)

# Upstream lineage is frozen from
# docs/references/strategy_fm_v1_5_字段手册.md §3. The v0.18 engine reads the
# StarRocks mirror for connectivity, but its field meaning comes from the
# corresponding Hive table(s), never from a v1.5 calculated result.
HIVE_SOURCE_BY_MIRROR: dict[str, tuple[str, ...]] = {
    "strategy_fm_sales_di": ("hive.dsl.dsl_transaction_non_daily_store_order_details_di",),
    "strategy_fm_purchase_di": ("hive.dsl.dsl_transaction_non_daily_store_article_purchase_di",),
    "strategy_fm_scm_di": ("hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di",),
    "strategy_fm_scm_adjust_di": ("hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di",),
    "strategy_fm_loss_di": ("hive.dal.dal_transaction_store_article_lost_di",),
    "strategy_fm_compose_di": ("hive.dsl.dsl_transaction_sotre_article_compose_info_di",),
    "strategy_fm_allowance_di": ("hive.dal.dal_activity_article_order_sale_info_di",),
    "strategy_fm_promo_di": ("hive.dsl.dsl_promotion_order_item_article_sale_info_di",),
    "strategy_fm_inventory_pool_di": ("hive.ods_sc_db.t_shop_inventory_sku_pool",),
    "strategy_fm_price_da": ("hive.dim.dim_store_article_price_info_da",),
    "strategy_fm_dim_day_clear": ("hive.dim.dim_day_clear_article_list_di",),
    "strategy_fm_dim_store_profile": ("hive.dim.dim_store_profile",),
    "strategy_fm_purchase_order_tmp": ("hive.ods_sc_db.t_purchase_order_item_tmp",),
    "strategy_fm_dim_goods": ("hive.dim.dim_goods_information_have_pt",),
    "strategy_fm_receive_sale_di": ("hive.dal.dal_receive_sale_di",),
    "strategy_fm_order_receive_di": ("hive.dal.dal_store_order_receive_di",),
    "strategy_fm_dim_article_convert": ("hive.dim.dim_store_article_convert_info_da",),
    "strategy_dim_store_article_bom_relation": ("hive.dim.dim_store_article_bom_relation",),
    "strategy_fm_store_article_inventory_detail_di": (
        "hive.ddl.ddl_transaction_store_article_inventory_detail_di",
    ),
    "strategy_fm_full_link_article_di": (
        "hive.dal_full_link.dal_manage_full_link_store_dc_article_info_di",
    ),
    "strategy_fm_store_daily_di": ("hive.dal.dal_transaction_sale_store_daily_di",),
    "strategy_fm_article_sale_di": ("hive.dal.dal_transaction_store_article_sale_info_di",),
    "strategy_fm_chdj_article_di": (
        "hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di",
    ),
    "strategy_fm_dim_order_saleable": ("hive.dim.dim_store_article_order_sale_info_di",),
    "strategy_fm_dim_calendar": ("hive.dim.dim_calendar",),
    "strategy_fm_order_offline_di": (
        "hive.dsl.dsl_transaction_sotre_order_offline_details_di",
        "hive.ods_pay_db.t_trade",
    ),
    "strategy_fm_order_online_di": (
        "hive.dsl.dsl_transaction_sotre_order_online_details_di",
        "hive.ods_pay_db.t_trade",
    ),
    "strategy_fm_trade_user": ("hive.ods_pay_db.t_trade",),
}

# v1.5 reads this native StarRocks application table directly. It is not a
# Hive mirror target and therefore is deliberately separated from the 28-table
# sync allowlist instead of being silently presented as one.
AUXILIARY_STARROCKS_TABLES: tuple[str, ...] = (
    "cuihua_t_purchase_wastage",
    "article_group_id_Key",
)


EXTRACTION_CONTRACTS: dict[str, MirrorContract] = {
    "product_group": MirrorContract(
        name="article_group_id_Key",
        authority=MirrorAuthority.DIMENSION,
        partition_column="inc_day",
        store_column=None,
        projection=(
            "inc_day", "area_name", "article_group_id", "article_group_name",
            "article_id", "article_name", "spu_name",
            "category_level1_description",
        ),
        expected_grain=("inc_day", "article_id"),
        shard_key="article_id",
        shards=32,
        managed_by_sync_script=False,
        base_predicates=("area_name IS NULL",),
        note=(
            "Daily product identity snapshot. area_name IS NULL is allowed only for the "
            "A3XV local shadow and does not prove a conversion event or rate."
        ),
    ),
    "sales": MirrorContract(
        name="strategy_fm_sales_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "business_date", "inc_day", "order_id", "order_status",
            "abi_article_id", "day_clear", "qty", "qty_spec", "actual_weight",
            "sales_amt", "af19_sales_qty", "af19_sales_amt", "p_lp_sub_amt",
            "discount_amt", "hour_discount_amt", "promotion_amt", "online_flag",
            "business_source", "jielong_flag", "customer_id", "first_buy_flag",
            "return_sale_qty", "return_sale_amt",
        ),
        expected_grain=("source_row_hash", "duplicate_ordinal"),
        shard_key="order_id",
        shards=16,
        grain_stage="derived",
        note=(
            "Signed sales rows. qty_spec/sales_amt and return fields already contain negative "
            "return values; never flip their source signs globally."
        ),
    ),
    "store_receipt": MirrorContract(
        name="strategy_fm_purchase_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "business_date", "inc_day", "day_clear",
            "article_id", "article_name", "sale_article_id", "sale_article_name",
            "sale_article_qty", "sale_article_purchase_amt", "avg_inbound_price",
            "init_stock_qty", "init_stock_amt", "end_stock_qty", "end_stock_amt",
            "inventory_cost",
        ),
        expected_grain=(
            "store_id", "business_date", "article_id", "sale_article_id", "day_clear",
        ),
        shard_key="sale_article_id",
        shards=16,
        note=(
            "purchase_di provides startup opening quantity and amount plus the "
            "article_id to sale_article_id conversion direction. Its allocated "
            "receipt quantity and amount do not enter daily receipt or conversion posting."
        ),
    ),
    "supply_chain": MirrorContract(
        name="strategy_fm_scm_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "business_date", "inc_day", "article_id",
            "store_order_qty", "order_qty_payean", "order_amt",
            "original_outstock_qty", "promotion_outstock_qty", "gift_outstock_qty",
            "total_outstock_qty", "out_stock_pay_amt", "out_stock_pay_amt_notax",
            "out_stock_amt_cb", "out_stock_amt_cb_notax",
            "return_stock_qty", "return_stock_pay_amt", "return_stock_pay_amt_notax",
            "return_stock_amt_cb", "return_stock_amt_cb_notax",
            "scm_promotion_amt_total", "adjustment_amt",
        ),
        expected_grain=("store_id", "business_date", "article_id"),
        shard_key="article_id",
        shards=8,
        allow_empty=True,
        note=(
            "Supply-chain order/outbound/financial account only. Never post these amounts "
            "again as store receipt inventory. Return fields are already signed."
        ),
    ),
    "supply_chain_adjust": MirrorContract(
        name="strategy_fm_scm_adjust_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "business_date", "store_id", "dc_id", "matnr", "article_id",
            "tax", "adjustment_amt", "adjustment_amt_notax",
            "new_sp_store_id", "inc_day",
        ),
        expected_grain=("store_id", "business_date", "article_id", "dc_id"),
        shard_key="article_id",
        shards=4,
        allow_empty=True,
        note="Hive-mirror SCM difference adjustment; reporting only, never a store receipt.",
    ),
    "allowance": MirrorContract(
        name="strategy_fm_allowance_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "business_date", "inc_day", "sale_article_id",
            "split_sale_amt", "split_qty_spec", "split_sale_weight",
            "split_bf19_sale_amt", "split_bf19_sale_qty", "split_bf9_sale_weight",
            "split_bf19_cust_num", "split_cust_num", "split_p_lp_sub_amt",
            "split_discount_amt", "split_hour_discount_amt",
            "split_promotion_discount_amt", "split_return_sale_qty",
            "split_return_sale_amt", "split_order_qty", "split_order_amt",
            "split_allowance_amt",
        ),
        expected_grain=("source_row_hash", "duplicate_ordinal"),
        shard_key="sale_article_id",
        shards=16,
        grain_stage="derived",
        allow_empty=True,
        note=(
            "Hive-mirror activity allocation evidence. It supplies reporting metrics only; "
            "its profit, loss and inventory columns are deliberately not projected."
        ),
    ),
    "promotion": MirrorContract(
        name="strategy_fm_promo_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="shop_id",
        projection=(
            "inc_day", "shop_id", "sku_code", "order_id", "order_item_id",
            "order_status", "p_promo_amt", "p_promo_total_amt", "f_promo_amt",
            "f_promo_total_amt", "online_flag", "jielong_flag",
            "is_hour_promotion", "activity_code",
        ),
        expected_grain=("source_row_hash", "duplicate_ordinal"),
        shard_key="order_id",
        shards=16,
        grain_stage="derived",
        allow_empty=True,
        note="Promotion observation only; reporting aggregation must deduplicate order-item rows.",
    ),
    "article_sale": MirrorContract(
        name="strategy_fm_article_sale_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "business_date", "inc_day", "article_id", "business_flag",
            "sale_qty", "sale_amt", "sale_weight", "sale_piece_qty",
            "bf19_sale_qty", "bf19_sale_amt", "bf19_sale_weight",
            "bf19_sale_piece_qty", "cust_num", "bf19_cust_num",
            "online_cust_num", "offline_cust_num", "online_sale_qty",
            "offline_sale_qty", "online_sale_amt", "offline_sale_amt",
            "original_price_sale_amt", "discount_amt", "hour_discount_amt",
            "promotion_discount_amt", "return_sale_qty", "return_sale_amt",
        ),
        expected_grain=("store_id", "business_date", "article_id"),
        shard_key="article_id",
        shards=16,
        note=(
            "Hive-mirror SKU sales observation. Only sales, piece, channel, customer and "
            "discount fields are read; v1.5 profit/inventory/loss fields are excluded."
        ),
    ),
    "known_loss": MirrorContract(
        name="strategy_fm_loss_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "inc_day", "article_id", "know_lost_qty", "know_lost_amt",
            "unknow_lost_qty", "unknow_lost_amt",
        ),
        expected_grain=("store_id", "inc_day", "article_id"),
        shard_key="article_id",
        shards=4,
        allow_empty=True,
        note="Only know_lost_qty is a formal flow; upstream unknown loss is audit-only.",
    ),
    "inventory_detail": MirrorContract(
        name="strategy_fm_store_article_inventory_detail_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="shop_id",
        projection=(
            "shop_id", "inventory_date", "inc_day", "sku_code", "sale_stock_qty",
            "actual_stock_qty", "profit_loss_qty", "created_by", "created_at",
            "updated_by", "updated_at",
        ),
        expected_grain=("shop_id", "inventory_date", "sku_code"),
        shard_key="sku_code",
        shards=4,
        allow_empty=True,
        note=(
            "Only a nonnegative actual_stock_qty with explicit creator/updater operator "
            "evidence may overwrite the ledger. System snapshots are audit-only."
        ),
    ),
    "chdj_day_clear": MirrorContract(
        name="strategy_fm_chdj_article_di",
        authority=MirrorAuthority.REFERENCE_ONLY,
        partition_column="inc_day",
        store_column="store_id",
        projection=("store_id", "inc_day", "article_id", "day_clear"),
        expected_grain=("store_id", "inc_day", "article_id"),
        shard_key="article_id",
        shards=4,
        note=(
            "v1.5-compatible day_clear label. strategy_fm_dim_day_clear is a full-goods "
            "snapshot and row presence must not be interpreted as day_clear=0."
        ),
    ),
    "day_clear": MirrorContract(
        name="strategy_fm_dim_day_clear",
        authority=MirrorAuthority.DIMENSION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "business_date", "store_id", "article_id", "article_name",
            "category_level1_id", "category_level1_description",
            "category_level2_id", "category_level2_description",
            "category_level3_id", "category_level3_description", "inc_day",
        ),
        expected_grain=("store_id", "business_date", "article_id"),
        shard_key="article_id",
        shards=8,
        allow_empty=True,
        base_predicates=("article_id IS NOT NULL",),
        note=(
            "Hive day-clear list. The live mirror has no day_clear value column: row presence "
            "means day_clear=0. Blank source rows without an article_id are not membership "
            "evidence and are excluded before grain validation. Absence falls back to the "
            "dated sales label, then default 1."
        ),
    ),
    "store_daily": MirrorContract(
        name="strategy_fm_store_daily_di",
        authority=MirrorAuthority.REFERENCE_ONLY,
        partition_column="inc_day",
        store_column="store_id",
        projection=("store_id", "inc_day", "bf19_sale_amt"),
        expected_grain=("store_id", "inc_day"),
        note="Only authorizes valid-business-day selection.",
    ),
    "order_offline": MirrorContract(
        name="strategy_fm_order_offline_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "business_date", "inc_day", "store_id", "order_id", "serial_id",
            "root_order_id", "afs_order_id", "je_order_id", "rje_order_id",
            "pay_at", "abi_article_id", "order_status", "jielong_flag",
            "sales_amt", "qty", "thirdparty_user_identity", "first_buy_flag",
        ),
        expected_grain=("source_row_hash", "duplicate_ordinal"),
        shard_key="order_id",
        shards=16,
        allow_empty=True,
        grain_stage="derived",
        note="Signed events; raw duplicate multiplicity is preserved.",
    ),
    "order_online": MirrorContract(
        name="strategy_fm_order_online_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "business_date", "inc_day", "store_id", "order_id", "serial_id",
            "root_order_id", "afs_order_id", "je_order_id", "rje_order_id",
            "pay_at", "abi_article_id", "order_status", "jielong_flag",
            "sales_amt", "qty", "thirdparty_user_identity", "first_buy_flag",
        ),
        expected_grain=("source_row_hash", "duplicate_ordinal"),
        shard_key="order_id",
        shards=8,
        allow_empty=True,
        grain_stage="derived",
        note="Signed events; raw duplicate multiplicity is preserved.",
    ),
    "trade_user": MirrorContract(
        name="strategy_fm_trade_user",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column=None,
        projection=("inc_day", "order_id", "thirdparty_user_identity", "trade_time"),
        expected_grain=("inc_day", "order_id"),
        shard_key="order_id",
        shards=8,
        allow_empty=True,
    ),
    "compose": MirrorContract(
        name="strategy_fm_compose_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "business_date", "inc_day", "article_id",
            "compose_in_qty", "compose_out_qty", "compose_in_amt", "compose_out_amt",
        ),
        expected_grain=("store_id", "business_date", "article_id"),
        shard_key="article_id",
        shards=4,
        allow_empty=True,
    ),
    "receive_sale": MirrorContract(
        name="strategy_fm_receive_sale_di",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "inc_day", "article_id", "article_name",
            "sale_article_id", "sale_article_name", "inbound_qty", "inbound_amount",
            "sum_article_qty", "sum_sub_article_qty", "sale_article_qty",
            "spilit_sale_article_amt", "rate", "sale_recev_rate",
            "category_level1_description",
        ),
        expected_grain=("store_id", "inc_day", "article_id", "sale_article_id"),
        shard_key="article_id",
        shards=8,
        allow_empty=True,
        note=(
            "Authoritative accepted A quantity and amount. Repeated A values across B rows "
            "must agree and are posted once; child quantities support BOM only."
        ),
    ),
    "bom_relation": MirrorContract(
        name="strategy_dim_store_article_bom_relation",
        authority=MirrorAuthority.DIMENSION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "inc_day", "parent_article_id", "parent_article_unit",
            "sub_article_id", "sub_article_unit", "dressing_rate", "cost_rate",
            "bom_type", "split_mode", "sp_level", "category_level1_description",
        ),
        expected_grain=("store_id", "inc_day", "parent_article_id", "sub_article_id"),
        shard_key="parent_article_id",
        shards=16,
    ),
    "article_convert": MirrorContract(
        name="strategy_fm_dim_article_convert",
        authority=MirrorAuthority.DIMENSION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "inc_day", "parent_article_id", "parent_article_name",
            "sub_article_id", "sub_article_name", "parent_rate", "sub_rate", "ctype",
        ),
        expected_grain=("store_id", "inc_day", "parent_article_id", "sub_article_id"),
        shard_key="parent_article_id",
        shards=16,
    ),
    "order_receive": MirrorContract(
        name="strategy_fm_order_receive_di",
        authority=MirrorAuthority.DERIVED_BRIDGE,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "business_date", "inc_day", "order_article_id", "re_article_id",
            "order_qty", "order_amt", "rate", "type", "re_order_qty", "re_order_amt",
        ),
        expected_grain=("store_id", "inc_day", "order_article_id", "re_article_id"),
        required=False,
        shard_key="order_article_id",
        shards=4,
        allow_empty=True,
        note="Historical coverage currently ends 2025-11-17.",
    ),
    "order_saleability": MirrorContract(
        name="strategy_fm_dim_order_saleable",
        authority=MirrorAuthority.DIMENSION,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "store_id", "inc_day", "effective_date", "article_id", "article_name",
            "is_order", "saleable", "status", "vendor_id", "vendor_name",
            "order_base", "min_order_base", "max_order_base",
        ),
        expected_grain=("store_id", "inc_day", "article_id"),
        shard_key="article_id",
        shards=8,
        note=(
            "Authoritative daily flags from v1.5 sync step 26. is_order and "
            "saleable are independent and must not be collapsed."
        ),
    ),
    "goods": MirrorContract(
        name="strategy_fm_dim_goods",
        authority=MirrorAuthority.DIMENSION,
        partition_column="inc_day",
        store_column=None,
        projection=(
            "inc_day", "article_id", "article_name", "sale_unit", "unit_weight",
            "matnr", "matnr_unit", "order_unit", "atob_value", "zglfz", "zglfm",
            "category_level1_id", "category_level1_description",
            "category_level2_id", "category_level2_description",
            "category_level3_id", "category_level3_description",
            "spu_id", "spu_name", "blackwhite_pig_id", "blackwhite_pig_name",
        ),
        expected_grain=("inc_day", "article_id"),
        shard_key="article_id",
        shards=32,
        partition_mode=PartitionMode.LATEST_SNAPSHOT,
    ),
    "store_profile": MirrorContract(
        name="strategy_fm_dim_store_profile",
        authority=MirrorAuthority.DIMENSION,
        partition_column="inc_day",
        store_column="sp_store_id",
        projection=(
            "inc_day", "sp_store_id", "sp_store_name", "store_flag_name",
            "manage_area_name", "sap_area_name", "city_description",
        ),
        expected_grain=("inc_day", "sp_store_id"),
        partition_mode=PartitionMode.LATEST_SNAPSHOT,
        note="Latest Hive-mirror store dimension used only for report dimensions.",
    ),
    "price": MirrorContract(
        name="strategy_fm_price_da",
        authority=MirrorAuthority.REFERENCE_ONLY,
        partition_column="inc_day",
        store_column="shop_id",
        projection=(
            "id", "shop_id", "sku_code", "current_price", "yesterday_price",
            "original_price", "unadjust_sale_price", "anchor_sale_price",
            "dc_price", "dc_original_price", "dc_original_price_sap",
            "original_dc_price", "yesterday_dc_price", "outstock_addprice_amt",
            "outstock_addprice_rate", "outstock_profit_rate", "outstock_lock_price",
            "deal_status", "calc_status", "confirm_status", "sale_status",
            "lock_status", "effective", "is_new", "inc_day",
        ),
        expected_grain=("source_row_hash", "duplicate_ordinal"),
        shard_key="sku_code",
        shards=8,
        grain_stage="derived",
        allow_empty=True,
        note=(
            "Hive-mirror selling/DC price observation for audit only. The live mirror can "
            "contain byte-identical duplicate rows for one id/SKU/day, so it is not joined "
            "into reporting metrics until a deterministic source snapshot rule is available."
        ),
    ),
    "inventory_pool": MirrorContract(
        name="strategy_fm_inventory_pool_di",
        authority=MirrorAuthority.REFERENCE_ONLY,
        partition_column="inc_day",
        store_column="shop_id",
        projection=(
            "inc_day", "inventory_date", "shop_id", "sku_code", "sku_name",
            "sales_unit", "weight_flag", "cost_price", "seven_days_avg_sale",
            "gift_flag", "spec", "sub_category_id", "sub_category_name",
        ),
        expected_grain=("shop_id", "inventory_date", "sku_code"),
        shard_key="sku_code",
        # 该表仅保留最新 inc_day 分区，且单分区含全历史 inventory_date
        # （2026-07-30 为 321 日 × ~3031 SKU ≈ 50.5 万行，每日约 +3000 行）。
        # shard 键 sku_code 只有 ~3031 个去重值、每值自带全部日期，分桶偏斜明显
        # （32 shard 实测最大桶 20276 行超线）；64 shard 平均 ~7900 行/桶，留足偏斜余量。
        shards=64,
        partition_mode=PartitionMode.LATEST_SNAPSHOT,
        allow_empty=True,
        note=(
            "Hive inventory-pool snapshot. cost_price is an external observation only; "
            "v0.18 must calculate weighted unit cost from its own ledger."
        ),
    ),
    "purchase_order_snapshot": MirrorContract(
        name="strategy_fm_purchase_order_tmp",
        authority=MirrorAuthority.REFERENCE_ONLY,
        partition_column="inc_day",
        store_column="shop_id",
        projection=(
            "inc_day", "shop_id", "sku_code", "receive_sku_code", "spu_code",
            "purchase_flag", "allow_single_order", "basic_qty", "min_batch_qty",
            "max_batch_qty", "propose_qty", "order_qty", "order_amount",
            "order_weight", "purchase_price", "sell_unit", "pur_unit",
            "unit_convert_mole", "unit_convert_deno", "conver_rate", "order_at",
            "is_deleted",
        ),
        expected_grain=("shop_id", "inc_day", "sku_code"),
        shard_key="sku_code",
        shards=8,
        partition_mode=PartitionMode.LATEST_SNAPSHOT,
        allow_empty=True,
        base_predicates=("IFNULL(is_deleted, 0) = 0",),
        note=(
            "Hive purchase-order process snapshot. It supplies order parameters and audit "
            "evidence only; purchase_flag is not saleable and this is not a complete order ledger."
        ),
    ),
    "full_link_article_reference": MirrorContract(
        name="strategy_fm_full_link_article_di",
        authority=MirrorAuthority.REFERENCE_ONLY,
        partition_column="inc_day",
        store_column="store_id",
        projection=(
            "business_date", "inc_day", "store_id", "store_name", "operate_id",
            "operate_name", "area_id", "area_name", "dc_id", "dc_name",
            "article_id", "article_name", "matnr", "first_category_id",
            "first_category_name", "second_category_id", "second_category_name",
            "third_category_id", "third_category_name", "business_flag",
        ),
        expected_grain=("store_id", "business_date", "article_id", "dc_id"),
        shard_key="article_id",
        shards=32,
        allow_empty=True,
        note=(
            "Hive full-link reference restricted to organization, goods and business-status "
            "dimensions. Its v1.5 inventory, loss, cost and profit outputs are forbidden inputs."
        ),
    ),
    "calendar": MirrorContract(
        name="strategy_fm_dim_calendar",
        authority=MirrorAuthority.DIMENSION,
        partition_column=None,
        store_column=None,
        projection=(
            "day_wid", "day_name", "day_date_chn", "day_date", "day_name_of_week",
            "day_of_week", "day_of_month", "day_of_year", "week_wid", "week_name",
            "week_no", "week_start_date", "week_end_date", "month_wid", "month_name",
            "month_no", "month_days", "month_start_date", "month_end_date",
            "quarter_wid", "quarter_name", "quarter_no", "quarter_start_date",
            "quarter_end_date", "year_wid", "year_name", "year_start_date",
            "year_end_date",
        ),
        expected_grain=("day_wid",),
        partition_mode=PartitionMode.STATIC_FULL,
        note="Hive calendar dimension. Join by the documented date field, never by guessed text conversion.",
    ),
    "purchase_wastage": MirrorContract(
        name="cuihua_t_purchase_wastage",
        authority=MirrorAuthority.OBSERVATION,
        partition_column="inc_day",
        store_column=None,
        projection=(
            "inc_day", "sku_code", "created_at", "reason", "waste_money",
            "waste_num", "is_deleted",
        ),
        expected_grain=("source_row_hash", "duplicate_ordinal"),
        shard_key="sku_code",
        shards=8,
        managed_by_sync_script=False,
        partition_mode=PartitionMode.LATEST_SNAPSHOT,
        allow_empty=True,
        grain_stage="derived",
        base_predicates=("is_deleted = 0",),
        note="Native StarRocks application source used by v1.5; v0.13 assigns A3XV explicitly.",
    ),
}
