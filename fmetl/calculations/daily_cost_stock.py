from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace
import math
from collections.abc import Sequence


@dataclass(frozen=True)
class DailyFlow:
    init_qty: float = 0.0
    init_amt: float = 0.0
    store_receive_qty: float = 0.0
    store_receive_amt: float = 0.0
    bom_in_qty: float = 0.0
    bom_in_amt: float = 0.0
    bom_out_qty: float = 0.0
    bom_out_amt: float = 0.0
    pack_in_qty: float = 0.0
    pack_in_amt: float = 0.0
    pack_out_qty: float = 0.0
    pack_out_amt: float = 0.0
    compose_in_qty: float = 0.0
    compose_in_amt: float = 0.0
    compose_out_qty: float = 0.0
    compose_out_amt: float = 0.0
    residual_transfer_in_qty: float = 0.0
    residual_transfer_in_amt: float = 0.0
    residual_transfer_out_qty: float = 0.0
    residual_transfer_out_amt: float = 0.0
    store_return_qty: float = 0.0
    sale_return_qty: float = 0.0
    sale_return_amt: float = 0.0
    inventory_gain_qty: float = 0.0
    inventory_gain_amt: float = 0.0
    sale_qty: float = 0.0
    known_lost_qty: float = 0.0
    actual_stock_qty: float | None = None
    is_counted: bool = False
    day_clear: str = "1"
    fallback_cost: float = 0.0


@dataclass(frozen=True)
class DailyState:
    issue_unit_cost: float
    ending_unit_cost: float
    sale_cost_amt: float
    known_lost_amt: float
    bom_out_amt: float
    pack_out_amt: float
    compose_out_amt: float
    residual_transfer_out_amt: float
    store_return_amt: float
    unknown_lost_qty: float
    balance_unknown_qty: float
    unknown_lost_amt: float
    balance_unknown_amt: float
    neg_clamp_qty: float
    neg_clamp_cost_amt: float
    end_qty: float
    end_amt: float
    eq_qty: float
    eq_amt: float
    qty_balance_residual: float
    amount_balance_residual: float
    branch: str


def _finite(value: float, name: str) -> float:
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{name} is not finite")
    return value


def transition_day(flow: DailyFlow) -> DailyState:
    """Apply one SKU/day transition after all formal internal flows are posted."""
    if flow.day_clear not in {"0", "1"}:
        raise ValueError(f"day_clear must be '0' or '1', got {flow.day_clear!r}")
    values = {name: _finite(value, name) for name, value in flow.__dict__.items()
              if isinstance(value, (int, float)) and not isinstance(value, bool)}
    actual = None if flow.actual_stock_qty is None else _finite(flow.actual_stock_qty, "actual_stock_qty")
    if actual is not None and actual < 0:
        raise ValueError("actual_stock_qty cannot be negative")
    nonnegative = {
        "init_qty", "init_amt", "store_receive_qty", "store_receive_amt",
        "bom_in_qty", "bom_in_amt", "bom_out_qty", "bom_out_amt",
        "pack_in_qty", "pack_in_amt", "pack_out_qty", "pack_out_amt",
        "compose_in_qty", "compose_in_amt", "compose_out_qty", "compose_out_amt",
        "residual_transfer_in_qty", "residual_transfer_in_amt",
        "residual_transfer_out_qty", "residual_transfer_out_amt",
        "store_return_qty",
        "sale_return_qty", "sale_return_amt", "inventory_gain_qty", "inventory_gain_amt",
        "sale_qty", "known_lost_qty", "fallback_cost",
    }
    bad = sorted(name for name in nonnegative if values.get(name, 0.0) < 0)
    if bad:
        raise ValueError(f"daily flow fields cannot be negative: {bad}")
    if actual is not None and (flow.inventory_gain_qty > 0 or flow.inventory_gain_amt > 0):
        raise ValueError("actual stock and inventory_gain cannot both adjust the same day")

    inflow_qty = (
        flow.init_qty + flow.store_receive_qty + flow.bom_in_qty + flow.pack_in_qty
        + flow.compose_in_qty + flow.residual_transfer_in_qty
        + flow.sale_return_qty + flow.inventory_gain_qty
    )
    inflow_amt = (
        flow.init_amt + flow.store_receive_amt + flow.bom_in_amt + flow.pack_in_amt
        + flow.compose_in_amt + flow.residual_transfer_in_amt
        + flow.sale_return_amt + flow.inventory_gain_amt
    )
    internal_out_qty = (
        flow.bom_out_qty + flow.pack_out_qty + flow.compose_out_qty
        + flow.residual_transfer_out_qty + flow.store_return_qty
    )
    if internal_out_qty > inflow_qty + 0.001:
        raise ValueError("formal internal out quantity exceeds the available pool")
    if inflow_qty <= 0.001 and abs(inflow_amt) > 0.01:
        raise ValueError("inventory pool has amount without quantity")
    issue_cost = inflow_amt / inflow_qty if inflow_qty > 0 else max(0.0, flow.fallback_cost)
    priced_out = {
        "bom_out_amt": flow.bom_out_qty * issue_cost,
        "pack_out_amt": flow.pack_out_qty * issue_cost,
        "compose_out_amt": flow.compose_out_qty * issue_cost,
        "residual_transfer_out_amt": flow.residual_transfer_out_qty * issue_cost,
        "store_return_amt": flow.store_return_qty * issue_cost,
    }
    supplied_out = {
        "bom_out_amt": flow.bom_out_amt,
        "pack_out_amt": flow.pack_out_amt,
        "compose_out_amt": flow.compose_out_amt,
        "residual_transfer_out_amt": flow.residual_transfer_out_amt,
    }
    mismatched = [
        name for name, supplied in supplied_out.items()
        if abs(supplied) > 0.01 and abs(supplied - priced_out[name]) > 0.01
    ]
    if mismatched:
        raise ValueError(
            "formal internal out amounts must equal quantity times current issue cost: "
            f"{mismatched}"
        )
    internal_out_amt = sum(priced_out.values())
    sale_cost = flow.sale_qty * issue_cost
    known_lost_amt = flow.known_lost_qty * issue_cost
    eq_qty = (
        inflow_qty - internal_out_qty
        - flow.sale_qty - flow.known_lost_qty
    )
    eq_amt = (
        inflow_amt - internal_out_amt
        - sale_cost - known_lost_amt
    )

    unknown_qty = 0.0
    neg_clamp_qty = 0.0
    if flow.is_counted:
        if actual is None:
            raise ValueError("is_counted requires actual_stock_qty")
        end_qty = actual
        unknown_qty = eq_qty - actual
        branch = "counted"
    elif flow.day_clear == "0":
        new_supply = (
            flow.store_receive_qty + flow.bom_in_qty - flow.bom_out_qty
            + flow.pack_in_qty - flow.pack_out_qty
            + flow.compose_in_qty - flow.compose_out_qty
            + flow.residual_transfer_in_qty - flow.residual_transfer_out_qty
            - flow.store_return_qty
            + flow.sale_return_qty + flow.inventory_gain_qty
        )
        end_qty = max(0.0, flow.init_qty - max(0.0, flow.sale_qty + flow.known_lost_qty - new_supply))
        unknown_qty = new_supply - flow.sale_qty - flow.known_lost_qty
        branch = "day_clear"
    elif eq_qty < 0:
        end_qty = 0.0
        unknown_qty = eq_qty
        neg_clamp_qty = -eq_qty
        branch = "negative_clamp"
    elif flow.known_lost_qty > 0:
        end_qty = eq_qty
        branch = "known_loss"
    elif actual is not None and actual > eq_qty + 0.001:
        end_qty = actual
        unknown_qty = eq_qty - actual
        branch = "snapshot_gain"
    else:
        end_qty = eq_qty
        branch = "normal"

    if branch in {"counted", "day_clear"}:
        end_amt = end_qty * issue_cost
        unknown_amt = eq_amt - end_amt
    elif branch == "negative_clamp":
        end_amt = 0.0
        unknown_amt = unknown_qty * issue_cost
    elif branch == "snapshot_gain":
        end_amt = end_qty * issue_cost
        unknown_amt = eq_amt - end_amt
    else:
        end_amt = eq_amt
        unknown_amt = 0.0
    neg_clamp_amt = neg_clamp_qty * issue_cost
    if end_amt < -0.01:
        raise ValueError(f"daily state produced negative ending amount: {end_amt}")
    if end_qty <= 0.001 and abs(end_amt) > 0.01:
        raise ValueError("daily state produced ending amount without ending quantity")
    ending_cost = end_amt / end_qty if end_qty > 0 else issue_cost
    if issue_cost < 0 or ending_cost < 0:
        raise ValueError("daily state produced a negative unit cost")
    # Display loss and accounting balance are separate. In soft day-clear the
    # display metric can describe opening-stock consumption even when the
    # equation residual is different (for example when demand exceeds all
    # available stock). The balance posting is always the actual residual.
    balance_unknown_qty = eq_qty - end_qty
    balance_unknown_amt = eq_amt - end_amt
    qty_residual = eq_qty - end_qty - balance_unknown_qty
    amount_residual = eq_amt - end_amt - balance_unknown_amt
    return DailyState(
        issue_unit_cost=issue_cost,
        ending_unit_cost=ending_cost,
        sale_cost_amt=sale_cost,
        known_lost_amt=known_lost_amt,
        bom_out_amt=priced_out["bom_out_amt"],
        pack_out_amt=priced_out["pack_out_amt"],
        compose_out_amt=priced_out["compose_out_amt"],
        residual_transfer_out_amt=priced_out["residual_transfer_out_amt"],
        store_return_amt=priced_out["store_return_amt"],
        unknown_lost_qty=unknown_qty,
        balance_unknown_qty=balance_unknown_qty,
        unknown_lost_amt=unknown_amt,
        balance_unknown_amt=balance_unknown_amt,
        neg_clamp_qty=neg_clamp_qty,
        neg_clamp_cost_amt=neg_clamp_amt,
        end_qty=max(0.0, end_qty),
        end_amt=end_amt,
        eq_qty=eq_qty,
        eq_amt=eq_amt,
        qty_balance_residual=qty_residual,
        amount_balance_residual=amount_residual,
        branch=branch,
    )


def roll_forward_days(
    activities: Sequence[DailyFlow],
    *,
    initial_qty: float,
    initial_amt: float,
) -> list[DailyState]:
    """Carry exact ending quantity/amount into the next daily weighted pool."""
    opening_qty = _finite(initial_qty, "initial_qty")
    opening_amt = _finite(initial_amt, "initial_amt")
    if opening_qty < 0 or opening_amt < 0:
        raise ValueError("initial rolling inventory cannot be negative")
    if opening_qty <= 0.001 and abs(opening_amt) > 0.01:
        raise ValueError("initial rolling inventory has amount without quantity")
    states: list[DailyState] = []
    for activity in activities:
        if abs(activity.init_qty) > 0.000001 or abs(activity.init_amt) > 0.000001:
            raise ValueError("roll_forward_days owns init_qty/init_amt; activities must leave them zero")
        state = transition_day(replace(activity, init_qty=opening_qty, init_amt=opening_amt))
        states.append(state)
        opening_qty, opening_amt = state.end_qty, state.end_amt
    return states
