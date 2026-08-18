from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd

from fmetl.calculations.special_wastage import build_wastage_trace
from fmetl.outputs.levels_result import ADDITIVE_INPUTS


def _as_text(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    output = frame.copy()
    for column in columns:
        if column in output:
            output[column] = output[column].astype(str)
    return output


def _tri_state(value: object) -> bool | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().lower()
    if text in {"1", "1.0", "true", "yes", "y"}:
        return True
    if text in {"0", "0.0", "false", "no", "n"}:
        return False
    return None


def _bool(value: object) -> bool:
    return _tri_state(value) is True


def _member_present(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    return str(value).strip().lower() not in {"", "-", "nan", "none", "null"}


def _jielong(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    text = str(value).strip().lower()
    # The order mirrors use ``shop`` for a group-buying order and ``-`` for a
    # normal online order.  Numeric/boolean true values remain supported.
    return text == "shop" or _tri_state(value) is True


def _normalize_day_clear(value: object) -> str | None:
    state = _tri_state(value)
    return None if state is None else ("1" if state else "0")


def _order_level_states(orders: pd.DataFrame) -> pd.DataFrame:
    """Resolve customer attributes once per order and reuse them for all metrics."""
    order_keys = ["store_id", "business_date", "order_id"]

    def unique_state(values: pd.Series) -> bool | None:
        states = {state for state in values.map(_tri_state) if state is not None}
        if len(states) > 1:
            raise ValueError("one order has conflicting first_buy_flag values")
        return next(iter(states)) if states else None

    def unique_channel(values: pd.Series) -> str:
        channels = sorted(set(values.dropna().astype(str)))
        if len(channels) != 1:
            raise ValueError("one order has conflicting online/offline channel values")
        return channels[0]

    return orders.groupby(order_keys, as_index=False).agg(
        pay_at=("pay_at", "min"),
        source_channel=("source_channel", unique_channel),
        order_first_buy_flag=("first_buy_flag", unique_state),
        order_jielong_flag=("jielong_flag", lambda values: any(values.map(_jielong))),
        has_member_identity=(
            "thirdparty_user_identity",
            lambda values: any(values.map(_member_present)),
        ),
    )


def build_scm_margin_audit(supply_chain: pd.DataFrame) -> pd.DataFrame:
    """Show the published outbound margin beside the return-inclusive margin."""
    columns = [
        "store_id", "business_date", "article_id",
        "outbound_margin_amt", "return_margin_amt",
        "return_inclusive_margin_amt", "return_inclusive_minus_outbound_amt",
        "published_scm_margin_rule",
    ]
    if supply_chain.empty:
        return pd.DataFrame(columns=columns)
    keys = ["store_id", "business_date", "article_id"]
    required = {
        *keys, "out_stock_pay_amt_notax", "out_stock_amt_cb_notax",
        "return_stock_pay_amt_notax", "return_stock_amt_cb_notax",
    }
    missing = sorted(required - set(supply_chain.columns))
    if missing:
        raise KeyError(f"supply-chain margin audit missing columns: {missing}")
    frame = _as_text(supply_chain, keys)
    amounts = sorted(required - set(keys))
    frame[amounts] = frame[amounts].apply(
        pd.to_numeric, errors="coerce"
    ).fillna(0.0)
    frame = frame.groupby(keys, as_index=False)[amounts].sum()
    frame["outbound_margin_amt"] = (
        frame["out_stock_pay_amt_notax"] - frame["out_stock_amt_cb_notax"]
    )
    frame["return_margin_amt"] = (
        frame["return_stock_pay_amt_notax"]
        - frame["return_stock_amt_cb_notax"]
    )
    frame["return_inclusive_margin_amt"] = (
        frame["outbound_margin_amt"] + frame["return_margin_amt"]
    )
    frame["return_inclusive_minus_outbound_amt"] = frame["return_margin_amt"]
    frame["published_scm_margin_rule"] = "OUTBOUND_ONLY_V15_COMPATIBLE"
    return frame[columns]


def build_reporting_metrics(
    *,
    activities: pd.DataFrame,
    mirrors: Mapping[str, pd.DataFrame],
    goods: pd.DataFrame,
    processing: pd.DataFrame,
    store_id: str,
) -> pd.DataFrame:
    keys = ["store_id", "business_date", "article_id"]
    base = activities[keys + ["day_clear", "store_receive_qty"]].copy()
    dimensions = goods.copy()
    dimensions["article_id"] = dimensions["article_id"].astype(str)
    dimensions = dimensions.drop_duplicates("article_id")
    keep = [
        "article_id", "article_name", "category_level1_id", "category_level1_description",
        "category_level2_description", "category_level3_description", "spu_id",
        "spu_name", "sale_unit", "unit_weight",
    ]
    base = base.merge(dimensions[keep], on="article_id", how="left", validate="many_to_one")
    missing_goods = base["article_name"].isna()
    if missing_goods.any():
        raise ValueError(
            "v0.18 activity SKU missing Hive goods snapshot: "
            f"{base.loc[missing_goods, 'article_id'].head(20).tolist()}"
        )
    profile = mirrors["store_profile"]
    profile_row = profile.loc[profile["sp_store_id"].astype(str).eq(store_id)]
    if profile_row.empty:
        raise ValueError(f"store profile missing {store_id}")
    profile_row = profile_row.iloc[0]
    # The v0.18 local run is deliberately scoped to A3XV. The v1.5 output
    # contract identifies that store through chdj_store_info as a Food Mart
    # store, while dim_store_profile describes it as a generic "new business"
    # store.  Keep the public dimensions compatible with the current v1.5
    # result instead of leaking the different profile taxonomy into the 123
    # field output.
    base["store_flag"] = "翠花店" if store_id == "A3XV" else str(
        profile_row.get("store_flag_name", "")
    )
    base["store_no"] = "food mart" if store_id == "A3XV" else store_id
    base["store_name"] = str(profile_row["sp_store_name"])
    base["sku_id"] = base["article_id"]
    base["sku_name"] = base["article_name"]
    base["is_reportable"] = ~base["category_level1_id"].astype(str).isin(
        {"70", "71", "72", "73", "74", "75", "76", "77"}
    )
    raw_ids = set(processing["raw_article_id"].astype(str)) if not processing.empty else set()
    base["is_processing_raw"] = base["article_id"].isin(raw_ids)
    observed_inventory = mirrors["store_receipt"][[
        "store_id", "business_date", "sale_article_id",
        "init_stock_amt", "end_stock_amt", "init_stock_qty", "end_stock_qty",
    ]].copy().rename(columns={
        "sale_article_id": "article_id",
        "init_stock_amt": "initial_inventory_amount",
        "end_stock_amt": "ending_inventory_amount",
    })
    base["has_observed_inventory"] = False
    if not observed_inventory.empty:
        observed_inventory = _as_text(observed_inventory, keys)
        inventory_columns = [
            "initial_inventory_amount", "ending_inventory_amount",
            "init_stock_qty", "end_stock_qty",
        ]
        observed_inventory[inventory_columns] = observed_inventory[
            inventory_columns
        ].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        observed_inventory = observed_inventory.groupby(
            keys, as_index=False
        )[inventory_columns].sum()
        observed_inventory["has_observed_inventory_observed"] = True
        base = base.merge(
            observed_inventory, on=keys, how="left", validate="one_to_one"
        )
        base["has_observed_inventory"] = base[
            "has_observed_inventory_observed"
        ].eq(True)
        base = base.drop(columns="has_observed_inventory_observed")
    article_sale = mirrors["article_sale"].copy()
    if not article_sale.empty:
        article_sale = _as_text(article_sale, keys)
        numeric = [column for column in article_sale.columns if column not in keys + ["inc_day", "business_flag"]]
        article_sale[numeric] = article_sale[numeric].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        article_sale = article_sale.groupby(keys, as_index=False)[numeric].sum()
        base = base.merge(article_sale, on=keys, how="left", validate="one_to_one")
        mapping = {
            "sale_weight": "sales_weight", "bf19_sale_weight": "sales_weight_before_19",
            "cust_num": "total_customer_count", "bf19_cust_num": "customer_count_before_19",
            "bf19_sale_amt": "sale_amount_before_19", "bf19_sale_qty": "sale_qty_before_19",
            "bf19_sale_piece_qty": "piece_sales_before_19", "sale_piece_qty": "sale_piece_qty",
            "online_cust_num": "online_customer_count", "offline_cust_num": "offline_customer_count",
            "online_sale_amt": "online_sale_amt", "offline_sale_amt": "offline_sale_amt",
            "online_sale_qty": "online_sale_qty", "offline_sale_qty": "offline_sale_qty",
            "original_price_sale_amt": "original_sale_amount",
            "discount_amt": "discount_amount", "hour_discount_amt": "time_period_discount_amount",
            "promotion_discount_amt": "promotional_discount_amount",
            "return_sale_qty": "return_rate_numerator", "sale_qty": "return_rate_denominator",
        }
        for source, target in mapping.items():
            if source in base:
                base[target] = pd.to_numeric(base[source], errors="coerce").fillna(0.0)

    inventory_pool = mirrors["inventory_pool"].rename(columns={
        "shop_id": "store_id", "inventory_date": "business_date",
        "sku_code": "article_id", "seven_days_avg_sale": "avg_7d_sale_qty",
    }).copy()
    if not inventory_pool.empty:
        inventory_pool = _as_text(inventory_pool, keys)
        avg_7d = inventory_pool.groupby(keys, as_index=False)[
            "avg_7d_sale_qty"
        ].max()
        avg_7d["avg_7d_sale_qty"] = pd.to_numeric(
            avg_7d["avg_7d_sale_qty"], errors="raise"
        )
        base = base.merge(avg_7d, on=keys, how="left", validate="one_to_one")

    price = mirrors["price"].rename(columns={
        "shop_id": "store_id", "inc_day": "business_date", "sku_code": "article_id",
    }).copy()
    base["original_price"] = np.nan
    base["dc_original_price"] = np.nan
    if not price.empty:
        price = _as_text(price, keys)
        price_values = ["original_price", "dc_original_price"]
        price[price_values] = price[price_values].apply(
            pd.to_numeric, errors="coerce"
        )
        conflicts = price.groupby(keys)[price_values].nunique(dropna=True).gt(1).any(axis=1)
        if conflicts.any():
            raise ValueError(
                "price mirror contains conflicting original-price evidence: "
                f"{conflicts[conflicts].index.tolist()[:20]}"
            )
        price = price.groupby(keys, as_index=False)[price_values].max()
        base = base.drop(columns=["original_price", "dc_original_price"]).merge(
            price, on=keys, how="left", validate="one_to_one"
        )

    order_parts: list[pd.DataFrame] = []
    for channel, source_name in (("offline", "order_offline"), ("online", "order_online")):
        orders = mirrors[source_name].copy()
        if orders.empty:
            continue
        if "first_buy_flag" not in orders:
            orders["first_buy_flag"] = None
        orders = orders.loc[
            orders["order_status"].astype(str).isin({"os.completed", "os.split"})
            & pd.to_numeric(orders["qty"], errors="coerce").fillna(0.0).gt(0)
        ].rename(columns={"abi_article_id": "article_id"})
        orders["source_channel"] = channel
        order_parts.append(orders)
    if order_parts:
        orders = pd.concat(order_parts, ignore_index=True)
        orders = _as_text(orders, keys)
        orders["pay_at"] = pd.to_datetime(orders["pay_at"], errors="coerce")
        orders["sales_amt"] = pd.to_numeric(orders["sales_amt"], errors="raise")
        orders["qty"] = pd.to_numeric(orders["qty"], errors="raise")
        order_keys = ["store_id", "business_date", "order_id"]
        order_states = _order_level_states(orders)
        orders = orders.drop(
            columns=["pay_at", "source_channel"], errors="ignore"
        ).merge(order_states, on=order_keys, how="left", validate="many_to_one")
        orders["is_new"] = orders["order_first_buy_flag"].eq(True)
        orders["is_old"] = (
            orders["order_first_buy_flag"].eq(False)
            & orders["has_member_identity"].map(bool)
        )
        orders["is_jielong_order"] = orders["order_jielong_flag"].map(bool)
        orders["is_jsd_order"] = orders["source_channel"].eq("online") & ~orders["is_jielong_order"]
        for prefix, mask in (("new_cust", orders["is_new"]), ("old_cust", orders["is_old"])):
            classified = orders.loc[mask].groupby(keys, as_index=False).agg(
                **{
                    f"{prefix}_num": ("order_id", "nunique"),
                    f"{prefix}_sale_amt": ("sales_amt", "sum"),
                    f"{prefix}_sale_qty": ("qty", "sum"),
                }
            )
            base = base.merge(classified, on=keys, how="left", validate="one_to_one")
        for prefix, mask in (("jielong", orders["is_jielong_order"]), ("jsd", orders["is_jsd_order"])):
            channel = orders.loc[mask].groupby(keys, as_index=False).agg(
                **{
                    f"{prefix}_sale_amt": ("sales_amt", "sum"),
                    f"{prefix}_sale_qty": ("qty", "sum"),
                }
            )
            base = base.drop(
                columns=[f"{prefix}_sale_amt", f"{prefix}_sale_qty"], errors="ignore"
            ).merge(channel, on=keys, how="left", validate="one_to_one")

    scm = mirrors["supply_chain"].copy()
    if not scm.empty:
        scm = _as_text(scm, keys)
        numeric = [column for column in scm.columns if column not in keys + ["inc_day"]]
        scm[numeric] = scm[numeric].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        scm = scm.groupby(keys, as_index=False)[numeric].sum()
        base = base.merge(scm, on=keys, how="left", validate="one_to_one")
        def value(name: str) -> pd.Series:
            if name in base:
                return pd.to_numeric(base[name], errors="coerce").fillna(0.0)
            return pd.Series(0.0, index=base.index)

        # SCM profit is the outbound distribution margin.  Return-stock
        # amounts are separate observations used by the return-rate metrics;
        # subtracting them here would count the return effect twice.
        out_cost = value("out_stock_amt_cb_notax")
        out_pay = value("out_stock_pay_amt_notax")
        base["outbound_cost"] = out_cost
        # purchase_weight is not a physical column in strategy_fm_scm_di.
        # Derive it from the actual receipt quantity and the authoritative
        # goods-unit conversion; order_qty_payean remains an order audit field.
        receive_qty = pd.to_numeric(
            base["store_receive_qty"], errors="raise"
        ).fillna(0.0)
        unit_weight = pd.to_numeric(
            base["unit_weight"], errors="coerce"
        ).fillna(0.0)
        unit_weight = unit_weight.mask(unit_weight.le(0), 1.0)
        base["purchase_weight_amount"] = np.where(
            base["sale_unit"].astype(str).eq("千克"),
            receive_qty,
            receive_qty * unit_weight,
        )
        base["supply_chain_promotion_amount"] = value("scm_promotion_amt_total")
        base["supply_chain_profit_amount"] = out_pay - out_cost
        base["supply_chain_profit_rate_denominator"] = out_pay
        expected_out = value("out_stock_pay_amt") + base["supply_chain_promotion_amount"]
        base["supply_chain_expected_profit_rate_numerator"] = expected_out - value(
            "out_stock_amt_cb"
        )
        base["supply_chain_expected_profit_rate_denominator"] = expected_out
        base["supply_chain_discount_rate_denominator"] = (
            base["supply_chain_promotion_amount"] + value("out_stock_pay_amt_notax")
        )
        base["return_rate_numerator"] = value("return_stock_pay_amt")
        base["return_rate_denominator"] = (
            value("out_stock_pay_amt") + value("return_stock_pay_amt")
        )

    saleable = mirrors["order_saleability"].rename(columns={"inc_day": "business_date"}).copy()
    if not saleable.empty:
        saleable = _as_text(saleable, keys)
        listed = saleable.groupby(keys)["saleable"].max().rename("listed").reset_index()
        base = base.merge(listed, on=keys, how="left", validate="one_to_one")
        base["stock_sku_flag"] = base["listed"].map(_bool).astype(float)
    sale_events = mirrors["sales"].rename(columns={"abi_article_id": "article_id"}).copy()
    if not sale_events.empty:
        sale_events = _as_text(sale_events, keys)
        positive_sale = pd.to_numeric(
            sale_events["qty_spec"], errors="coerce"
        ).fillna(0.0).gt(0)
        active_keys = sale_events.loc[positive_sale, keys].drop_duplicates().assign(
            active_sku_flag=1.0
        )
        base = base.merge(active_keys, on=keys, how="left", validate="one_to_one")
    if "active_sku_flag" not in base:
        base["active_sku_flag"] = 0.0
    base["active_sku_flag"] = pd.to_numeric(
        base["active_sku_flag"], errors="coerce"
    ).fillna(0.0)
    base["lp_sale_amt"] = (
        pd.to_numeric(base["original_price_sale_amt"], errors="coerce").fillna(0.0)
        if "original_price_sale_amt" in base else 0.0
    )
    order_parts = []
    for source_name in ("order_offline", "order_online"):
        orders = mirrors[source_name].copy()
        if orders.empty:
            continue
        orders = orders.loc[
            orders["order_status"].astype(str).isin({"os.completed", "os.split"})
            & pd.to_numeric(orders["qty"], errors="coerce").fillna(0.0).gt(0)
        ].rename(columns={"abi_article_id": "article_id"})
        order_parts.append(orders[["store_id", "business_date", "article_id", "pay_at"]])
    if order_parts:
        order_times = pd.concat(order_parts, ignore_index=True)
        order_times = _as_text(order_times, keys)
        order_times["pay_at"] = pd.to_datetime(order_times["pay_at"], errors="coerce")
        last_sale = order_times.groupby(keys, as_index=False)["pay_at"].max()
        last_sale["last_sale_hour"] = last_sale["pay_at"].dt.hour
        base = base.merge(
            last_sale[keys + ["last_sale_hour"]],
            on=keys, how="left", validate="one_to_one",
        )
    else:
        base["last_sale_hour"] = np.nan
    wastage = build_wastage_trace(mirrors["purchase_wastage"])
    if not wastage.empty:
        special = wastage.pivot_table(
            index=keys,
            columns="reason_code",
            values=["waste_money", "waste_num"],
            aggfunc="sum",
            fill_value=0.0,
        )
        special.columns = [
            f"{reason}_{'amt' if metric == 'waste_money' else 'qty'}"
            for metric, reason in special.columns
        ]
        special = special.reset_index()
        base = base.merge(special, on=keys, how="left", validate="one_to_one")
    # Event metrics may legitimately be zero when their authoritative mirror
    # has no row.  Formula metrics are calculated below or in the ledger
    # assembly and are never created by a catch-all zero fill.
    zero_when_no_event = {
        "sales_weight", "sales_weight_before_19", "sale_qty_before_19",
        "sale_amount_before_19", "piece_sales_before_19", "sale_piece_qty",
        "original_sale_amount", "discount_amount",
        "time_period_discount_amount", "promotional_discount_amount",
        "outbound_cost", "purchase_weight_amount",
        "supply_chain_promotion_amount", "supply_chain_profit_amount",
        "supply_chain_profit_rate_denominator",
        "supply_chain_expected_profit_rate_numerator",
        "supply_chain_expected_profit_rate_denominator",
        "supply_chain_discount_rate_denominator",
        "return_rate_numerator", "return_rate_denominator",
        "online_sale_amt", "offline_sale_amt", "online_sale_qty",
        "offline_sale_qty", "jielong_sale_amt", "jsd_sale_amt",
        "jielong_sale_qty", "jsd_sale_qty", "lp_sale_amt",
        "new_cust_num", "old_cust_num",
        "new_cust_sale_amt", "old_cust_sale_amt",
        "new_cust_sale_qty", "old_cust_sale_qty",
        "stock_sku_flag", "active_sku_flag", "avg_7d_sale_qty",
        "ccj_amt", "ccj_qty", "ssls_amt", "ssls_qty",
    }
    for column in zero_when_no_event:
        if column not in base:
            base[column] = 0.0
        base[column] = pd.to_numeric(
            base[column], errors="coerce"
        ).fillna(0.0)
    # Customer counts are replaced by distinct-order aggregation at each
    # public level; keeping zero placeholders here prevents accidental SKU
    # summation while preserving the frozen intermediate schema.
    for column in (
        "total_customer_count", "customer_count_before_19",
        "online_customer_count", "offline_customer_count",
        "jielong_customer_count", "jsd_customer_count",
    ):
        base[column] = 0.0
    formula_columns = {
        "store_expected_profit_amount", "store_pricing_profit_rate_numerator",
        "loss_rate_denominator", "loss_rate_qty_denominator",
        "soldout_16_numerator", "soldout_16_denominator",
        "soldout_20_numerator", "soldout_20_denominator",
        "initial_inventory_amount", "ending_inventory_amount",
        "init_stock_qty", "end_stock_qty", "loss_amount", "loss_qty",
        "store_know_lost_amt", "store_unknow_lost_amt", "inbound_amount",
        "inbound_qty", "total_sale_qty", "total_sale_amount",
        "accounting_profit", "accounting_full_profit",
        "ccj_ledger_cost_amt", "ssls_ledger_cost_amt",
    }
    unresolved = sorted(
        ADDITIVE_INPUTS
        - zero_when_no_event
        - formula_columns
        - {
            "total_customer_count", "customer_count_before_19",
            "online_customer_count", "offline_customer_count",
            "jielong_customer_count", "jsd_customer_count",
        }
    )
    if unresolved:
        raise RuntimeError(f"v0.18 reporting metric ownership is undefined: {unresolved}")
    required_dimensions = [
        "store_flag", "store_no", "store_name", "sku_id", "sku_name",
        "category_level1_description", "category_level2_description",
        "category_level3_description", "spu_id", "spu_name", "sale_unit", "day_clear",
        "is_processing_raw", "is_reportable", "has_observed_inventory",
    ]
    return base[
        keys + required_dimensions + sorted(ADDITIVE_INPUTS & set(base.columns))
        + ["original_price", "dc_original_price", "last_sale_hour"]
    ]


def build_customer_events(
    *,
    mirrors: Mapping[str, pd.DataFrame],
    goods: pd.DataFrame,
    day_clear: pd.DataFrame,
    store_id: str,
) -> pd.DataFrame:
    """Build the order-level counting spine used by every reporting level."""
    sales = mirrors["sales"].copy().rename(columns={"abi_article_id": "sku_id"})
    if sales.empty:
        raise ValueError("sales mirror cannot build customer events")
    sales["store_id"] = sales["store_id"].astype(str)
    sales["business_date"] = sales["business_date"].astype(str)
    sales["sku_id"] = sales["sku_id"].astype(str)
    sales["order_id"] = sales["order_id"].astype(str)
    authoritative_day_clear = day_clear.rename(
        columns={"article_id": "sku_id"}
    )[["store_id", "business_date", "sku_id", "day_clear"]].copy()
    authoritative_day_clear[["store_id", "business_date", "sku_id"]] = (
        authoritative_day_clear[["store_id", "business_date", "sku_id"]].astype(str)
    )
    sales = sales.drop(columns="day_clear", errors="ignore").merge(
        authoritative_day_clear,
        on=["store_id", "business_date", "sku_id"],
        how="left",
        validate="many_to_one",
    )
    qty = pd.to_numeric(sales["qty_spec"], errors="raise")
    sales = sales.loc[
        qty.gt(0)
        & sales["order_status"].astype(str).isin({"os.completed", "os.split"})
    ].copy()
    if sales.empty:
        raise ValueError("sales mirror has no positive completed order events")

    order_frames: list[pd.DataFrame] = []
    for channel, name in (("offline", "order_offline"), ("online", "order_online")):
        source = mirrors[name].copy()
        if source.empty:
            continue
        source = source.loc[
            source["order_status"].astype(str).isin({"os.completed", "os.split"})
        ].copy()
        source["source_channel"] = channel
        if "first_buy_flag" not in source:
            source["first_buy_flag"] = None
        order_frames.append(source[[
            "store_id", "business_date", "order_id", "abi_article_id",
            "pay_at", "source_channel", "first_buy_flag", "jielong_flag",
            "thirdparty_user_identity",
        ]].rename(columns={"abi_article_id": "sku_id"}))
    if not order_frames:
        raise ValueError("order mirrors are required for customer time/channel metrics")
    orders = pd.concat(order_frames, ignore_index=True)
    for column in ("store_id", "business_date", "order_id", "sku_id"):
        orders[column] = orders[column].astype(str)
    orders["pay_at"] = pd.to_datetime(orders["pay_at"], errors="coerce")
    order_keys = ["store_id", "business_date", "order_id"]
    orders = _order_level_states(orders)
    sales = sales.merge(
        orders, on=order_keys, how="left", validate="many_to_one"
    )
    missing_order = sales["pay_at"].isna() | sales["source_channel"].isna()
    if missing_order.any():
        sample = sales.loc[
            missing_order, [*order_keys, "sku_id"]
        ].drop_duplicates().head(20).to_dict("records")
        raise ValueError(f"customer events missing order time/channel evidence: {sample}")

    dims = goods[[
        "article_id", "article_name", "category_level1_id",
        "category_level1_description", "category_level2_description",
        "category_level3_description", "spu_id", "spu_name",
    ]].copy().rename(columns={"article_id": "sku_id", "article_name": "sku_name"})
    dims["sku_id"] = dims["sku_id"].astype(str)
    dims = dims.drop_duplicates("sku_id")
    sales = sales.merge(dims, on="sku_id", how="left", validate="many_to_one")
    if sales["sku_name"].isna().any():
        raise ValueError("customer event SKU is missing goods dimensions")
    sales = sales.loc[
        ~sales["category_level1_id"].astype(str).isin(
            {"70", "71", "72", "73", "74", "75", "76", "77"}
        )
    ].copy()
    profile = mirrors["store_profile"]
    profile_row = profile.loc[profile["sp_store_id"].astype(str).eq(store_id)]
    if profile_row.empty:
        raise ValueError(f"store profile missing {store_id}")
    sales["store_flag"] = "翠花店" if store_id == "A3XV" else str(
        profile_row.iloc[0].get("store_flag_name", "")
    )
    sales["store_no"] = "food mart" if store_id == "A3XV" else store_id
    sales["store_name"] = str(profile_row.iloc[0]["sp_store_name"])
    sales["day_clear"] = sales["day_clear"].map(_normalize_day_clear)
    if sales["day_clear"].isna().any():
        raise ValueError("customer events contain an invalid day_clear label")
    sales["is_before_19"] = sales["pay_at"].dt.hour.lt(19)
    sales["is_online"] = sales["source_channel"].eq("online")
    sales["is_offline"] = ~sales["is_online"]
    sales["is_jielong"] = sales["order_jielong_flag"].map(bool)
    sales["is_jsd"] = sales["is_online"] & ~sales["is_jielong"]
    sales["is_new"] = sales["order_first_buy_flag"].map(
        lambda value: _tri_state(value) is True
    )
    sales["is_old"] = sales["order_first_buy_flag"].map(
        lambda value: _tri_state(value) is False
    ) & sales["has_member_identity"].map(bool)
    columns = [
        "store_id", "store_flag", "store_no", "business_date", "store_name", "sku_id",
        "sku_name", "category_level1_description", "category_level2_description",
        "category_level3_description", "spu_id", "spu_name", "day_clear",
        "order_id", "is_before_19", "is_online", "is_offline", "is_jielong",
        "is_jsd", "is_new", "is_old",
    ]
    return sales[columns].drop_duplicates().reset_index(drop=True)
