from __future__ import annotations


def calculate_accounting_profit(
    *,
    sale_amt: float,
    store_receive_amt: float,
    bom_in_amt: float,
    bom_out_amt: float,
    pack_in_amt: float,
    pack_out_amt: float,
    compose_in_amt: float,
    compose_out_amt: float,
    init_stock_amt: float,
    end_stock_amt: float,
    residual_transfer_in_amt: float = 0.0,
    residual_transfer_out_amt: float = 0.0,
    neg_clamp_cost_amt: float = 0.0,
) -> float:
    """Consume finalized amount flows; do not recalculate costs here."""
    return float(
        sale_amt - store_receive_amt
        - bom_in_amt + bom_out_amt
        - pack_in_amt + pack_out_amt
        - compose_in_amt + compose_out_amt
        - residual_transfer_in_amt + residual_transfer_out_amt
        + end_stock_amt - init_stock_amt
        - neg_clamp_cost_amt
    )
