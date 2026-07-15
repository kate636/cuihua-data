from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

from fmetl.contracts.grains import assert_only_store


EVENT_STATUSES = {
    "os.completed",
    "os.split",
    "os.refund.completed",
    "os.return.completed",
}


def _source_hash(frame: pd.DataFrame) -> pd.Series:
    columns = sorted(frame.columns)

    def digest(row: pd.Series) -> str:
        payload = "\x1f".join("<NULL>" if pd.isna(row[col]) else str(row[col]) for col in columns)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    return frame.apply(digest, axis=1)


def normalize_order_events(offline: pd.DataFrame, online: pd.DataFrame) -> pd.DataFrame:
    """Build lossless signed order events and preserve exact duplicate rows."""
    required = {
        "business_date", "inc_day", "store_id", "order_id", "abi_article_id",
        "order_status", "pay_at", "jielong_flag", "sales_amt", "qty",
        "thirdparty_user_identity",
    }
    frames: list[pd.DataFrame] = []
    for channel, source in (("offline", offline), ("online", online)):
        missing = sorted(required - set(source.columns))
        if missing:
            raise KeyError(f"{channel} orders missing columns: {missing}")
        frame = source.copy()
        assert_only_store(frame, "A3XV")
        key_columns = [
            "business_date", "inc_day", "store_id", "order_id", "abi_article_id",
            "order_status", "pay_at",
        ]
        null_keys = frame[key_columns].isna() | frame[key_columns].astype(str).apply(
            lambda column: column.str.strip().eq("")
        )
        if null_keys.any().any():
            bad_columns = sorted(null_keys.columns[null_keys.any()].tolist())
            raise ValueError(f"{channel} orders contain NULL/blank key fields: {bad_columns}")
        frame = frame.loc[frame["order_status"].isin(EVENT_STATUSES)].copy()
        frame["source_channel"] = channel
        frame["canonical_order_key"] = (
            channel + "|" + frame["store_id"].astype(str) + "|"
            + frame["business_date"].astype(str) + "|" + frame["order_id"].astype(str)
        )
        frame["sales_amt"] = pd.to_numeric(frame["sales_amt"], errors="raise")
        frame["qty"] = pd.to_numeric(frame["qty"], errors="raise")
        if not np.isfinite(frame[["sales_amt", "qty"]].to_numpy(dtype=float)).all():
            raise ValueError(f"{channel} orders contain NULL/NaN/Inf sales_amt or qty")
        frame["source_row_hash"] = _source_hash(frame)
        frame["duplicate_ordinal"] = frame.groupby("source_row_hash", sort=False).cumcount() + 1
        frames.append(frame)
    result = pd.concat(frames, ignore_index=True)
    if result.duplicated(["source_channel", "source_row_hash", "duplicate_ordinal"]).any():
        raise ValueError("technical order key is not unique")
    return result


def join_trade_identity(events: pd.DataFrame, trade_user: pd.DataFrame) -> pd.DataFrame:
    """Attach one trade identity per inc_day/order without expanding rows."""
    required = {"inc_day", "order_id", "thirdparty_user_identity", "trade_time"}
    missing = sorted(required - set(trade_user.columns))
    if missing:
        raise KeyError(f"trade_user missing columns: {missing}")
    trade = trade_user[list(required)].copy()
    if trade[["inc_day", "order_id"]].isna().any().any():
        raise ValueError("trade_user contains NULL inc_day/order_id")
    identity_counts = trade.groupby(["inc_day", "order_id"], dropna=False)[
        "thirdparty_user_identity"
    ].nunique(dropna=True)
    conflicts = identity_counts[identity_counts > 1]
    if not conflicts.empty:
        raise ValueError(f"orders have multiple trade identities: {conflicts.head(10).to_dict()}")
    # Resolve the unique non-null identity independently from the earliest
    # trade timestamp; otherwise an early NULL row can hide a later identity.
    identities = (
        trade.dropna(subset=["thirdparty_user_identity"])
        .loc[lambda frame: frame["thirdparty_user_identity"].astype(str).str.strip().ne("")]
        .groupby(["inc_day", "order_id"], as_index=False)["thirdparty_user_identity"].first()
        .rename(columns={"thirdparty_user_identity": "trade_user_identity"})
    )
    timestamps = (
        trade.assign(trade_time=pd.to_datetime(trade["trade_time"], errors="raise"))
        .groupby(["inc_day", "order_id"], as_index=False)["trade_time"].min()
        .rename(columns={"trade_time": "trade_time_identity"})
    )
    trade = timestamps.merge(identities, on=["inc_day", "order_id"], how="left", validate="one_to_one")
    before = len(events)
    result = events.merge(trade, on=["inc_day", "order_id"], how="left", validate="many_to_one")
    if len(result) != before:
        raise ValueError(f"trade identity join expanded rows: before={before}, after={len(result)}")
    source_identity = result["thirdparty_user_identity"].replace("", pd.NA)
    trade_identity = result["trade_user_identity"].replace("", pd.NA)
    mismatch = source_identity.notna() & trade_identity.notna() & source_identity.ne(trade_identity)
    if mismatch.any():
        raise ValueError("order source identity conflicts with trade_user")
    result["customer_identity"] = source_identity.fillna(trade_identity)
    return result
