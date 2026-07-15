from __future__ import annotations

import pandas as pd
import numpy as np


def assert_daily_balances(
    states: pd.DataFrame,
    qty_tolerance: float = 0.001,
    amt_tolerance: float = 0.01,
    *,
    allow_empty: bool = False,
) -> None:
    required = {
        "qty_balance_residual", "amount_balance_residual", "end_qty", "end_amt",
        "issue_unit_cost", "ending_unit_cost",
    }
    missing = sorted(required - set(states.columns))
    if missing:
        raise KeyError(f"daily states missing columns: {missing}")
    if states.empty:
        if allow_empty:
            return
        raise ValueError("daily state result is empty")
    numeric = states[list(required)].apply(pd.to_numeric, errors="coerce")
    finite = pd.DataFrame(np.isfinite(numeric.to_numpy()), index=numeric.index, columns=numeric.columns)
    if not finite.all().all():
        bad_columns = sorted(finite.columns[~finite.all()].tolist())
        raise ValueError(f"daily states contain NULL/NaN/Inf: {bad_columns}")
    failures: list[str] = []
    if (numeric["end_qty"] < -qty_tolerance).any():
        failures.append("negative end stock")
    if (numeric["end_amt"] < -amt_tolerance).any():
        failures.append("negative ending amount")
    if (numeric[["issue_unit_cost", "ending_unit_cost"]] < 0).any().any():
        failures.append("negative unit cost")
    if (numeric["qty_balance_residual"].abs() > qty_tolerance).any():
        failures.append("quantity balance residual")
    if (numeric["amount_balance_residual"].abs() > amt_tolerance).any():
        failures.append("amount balance residual")
    if failures:
        raise ValueError("; ".join(failures))
