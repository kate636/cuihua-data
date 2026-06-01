"""strategy_fm_* 表体检脚本

用法:
    python -m fm_etl_v3.scripts.probe_strategy_fm [yyyy-mm-dd]

不传日期默认用前天（避开今日未完整入库的情况）。

对 15 张 strategy_fm_* 表分别执行：
  1. SHOW FULL COLUMNS FROM <table>  — 拿字段名 / 类型 / null / comment
  2. SELECT COUNT(*) / COUNT(DISTINCT store_id) / COUNT(DISTINCT article_id)
     FROM <table> WHERE <日期列> = <target_day>
  3. SELECT * FROM <table> WHERE <日期列> = <target_day> LIMIT 3  — 拿样本

产出：
  - 终端打印精简汇总（每张表一行）
  - 详细报告写到 fm_etl_v3/docs/strategy_fm_tables.md
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from ..connectors import ApiConnector
from ..utils import get_logger

_log = get_logger("probe_strategy_fm")

# (table, 日期过滤列, 预期粒度提示, 预期昨日行数)
TABLES: list[tuple[str, str, str, Optional[int]]] = [
    ("strategy_fm_sales_di",           "inc_day", "销售明细（订单行级？）",               1533),
    ("strategy_fm_purchase_di",        "inc_day", "进货验收（订单行级？）",               1821),
    ("strategy_fm_scm_di",             "inc_day", "SAP 出入库",                           300),
    ("strategy_fm_scm_adjust_di",      "inc_day", "SCM 差异调整（昨日可能为 0）",         0),
    ("strategy_fm_loss_di",            "inc_day", "损耗",                                 219),
    ("strategy_fm_compose_di",         "inc_day", "加工转换",                             22),
    ("strategy_fm_allowance_di",       "inc_day", "活动让利",                             943),
    ("strategy_fm_promo_di",           "inc_day", "促销（订单项）",                       1545),
    ("strategy_fm_inventory_pool_di",  "inc_day", "库存成本价池",                         244523),
    ("strategy_fm_price_da",           "inc_day", "门店商品价格",                         5009),
    ("strategy_fm_dim_day_clear",      "inc_day", "日清商品清单",                         92076),
    ("strategy_fm_dim_store_profile",  "inc_day", "门店画像",                             1),
    ("strategy_fm_dim_saleable",       "inc_day", "可售商品",                             1255),
    ("strategy_fm_dim_goods",          "inc_day", "商品主数据",                           92539),
    ("strategy_fm_dim_calendar",       "day_date", "日历维度（含全量）",                  None),
]

DOCS_PATH = Path(__file__).resolve().parent.parent / "docs" / "strategy_fm_tables.md"


def probe_schema(api: ApiConnector, table: str) -> pd.DataFrame:
    """拉取字段信息。优先用 SHOW FULL COLUMNS（含 comment 列），失败回退 DESCRIBE。"""
    try:
        df = api.query(f"SHOW FULL COLUMNS FROM {table}")
        # 标准化列顺序
        keep = [c for c in ["field", "type", "null", "key", "default", "comment"] if c in df.columns]
        return df[keep]
    except Exception as e:
        _log.warning(f"SHOW FULL COLUMNS failed on {table}: {str(e)[:120]}")
        df = api.query(f"DESCRIBE {table}")
        keep = [c for c in ["field", "type", "null", "key", "default"] if c in df.columns]
        return df[keep]


def _pick(row: dict, *candidates: str):
    """从 API 返回的 dict 里按候选名挑第一个存在的键。QDM API 会把 snake_case 转成 camelCase。"""
    for c in candidates:
        if c in row:
            return row[c]
    return None


def probe_counts(api: ApiConnector, table: str, date_col: str, day: str,
                 schema_fields: list[str]) -> dict:
    """拉取行数、去重 store/article 数。根据 schema 判断哪些 distinct 能算。

    注意：QDM API 会把 `AS total_rows` 别名改写成 `totalRows`（camelCase），所以读结果时要
    兼容两种写法。
    """
    selects = ["COUNT(*) AS total_rows"]
    store_col = next((c for c in ("store_id", "shop_id") if c in schema_fields), None)
    article_col = next(
        (c for c in ("article_id", "abi_article_id", "sale_article_id", "sku_code")
         if c in schema_fields),
        None,
    )
    if store_col:
        selects.append(f"COUNT(DISTINCT {store_col}) AS distinct_stores")
    if article_col:
        selects.append(f"COUNT(DISTINCT {article_col}) AS distinct_articles")

    sql = f"SELECT {', '.join(selects)} FROM {table} WHERE {date_col} = '{day}'"
    result = {"total_rows": None, "distinct_stores": None, "distinct_articles": None}
    try:
        raw = api.query(sql).iloc[0].to_dict()
        result["total_rows"]        = _pick(raw, "total_rows", "totalRows")
        result["distinct_stores"]   = _pick(raw, "distinct_stores", "distinctStores")
        result["distinct_articles"] = _pick(raw, "distinct_articles", "distinctArticles")
    except Exception as e:
        result["count_error"] = str(e)[:200]
        try:
            raw2 = api.query(f"SELECT COUNT(*) AS cnt FROM {table} WHERE {date_col} = '{day}'").iloc[0].to_dict()
            result["total_rows"] = _pick(raw2, "cnt")
        except Exception:
            pass
    result["_store_col_name"] = store_col
    result["_article_col_name"] = article_col
    return result


def probe_sample(api: ApiConnector, table: str, date_col: str, day: str, n: int = 3) -> pd.DataFrame:
    """拉取 N 条样本。dim_calendar 不过滤日期，取一条即可。"""
    if table == "strategy_fm_dim_calendar":
        sql = f"SELECT * FROM {table} LIMIT {n}"
    else:
        sql = f"SELECT * FROM {table} WHERE {date_col} = '{day}' LIMIT {n}"
    try:
        return api.query(sql)
    except Exception as e:
        _log.warning(f"sample failed on {table}: {str(e)[:120]}")
        return pd.DataFrame()


def render_schema_table(schema: pd.DataFrame) -> str:
    """输出 markdown 表格。"""
    if schema.empty:
        return "_（未能拉取字段信息）_"
    header_cols = list(schema.columns)
    lines = ["| " + " | ".join(header_cols) + " |",
             "|" + "|".join(["---"] * len(header_cols)) + "|"]
    for _, row in schema.iterrows():
        cells = []
        for c in header_cols:
            val = row[c]
            if pd.isna(val) or val is None:
                val = ""
            s = str(val).replace("|", "\\|").replace("\n", " ")
            if len(s) > 80:
                s = s[:77] + "..."
            cells.append(s)
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def render_sample(sample: pd.DataFrame) -> str:
    if sample.empty:
        return "_（无样本数据）_"
    # 只取前 15 列展示，避免 markdown 太宽
    shown = sample.iloc[:, :15].copy()
    for c in shown.columns:
        shown[c] = shown[c].astype(str).str.replace("|", "\\|", regex=False)
    return shown.to_markdown(index=False) if hasattr(shown, "to_markdown") else shown.to_string()


def assess_row_count(table: str, actual: Optional[int], expected: Optional[int]) -> str:
    """根据预期和实际行数给出评估结论。"""
    if actual is None:
        return "⚠️ 无法取到 COUNT(*)，需人工排查"
    if expected is None:
        return f"实测 {actual:,} 行（无基线参考）"
    if expected == 0:
        if actual == 0:
            return "✅ 实测 0，与预期一致"
        return f"⚠️ 预期 0，实测 {actual:,} 行，待排查"
    diff_pct = abs(actual - expected) / max(expected, 1) * 100
    if diff_pct <= 5:
        return f"✅ 预期 {expected:,}，实测 {actual:,}，误差 {diff_pct:.1f}%"
    elif diff_pct <= 20:
        return f"⚠️ 预期 {expected:,}，实测 {actual:,}，误差 {diff_pct:.1f}%（偏差可接受但需复核）"
    else:
        return f"❌ 预期 {expected:,}，实测 {actual:,}，误差 {diff_pct:.1f}%（偏差过大）"


def main(day: str) -> None:
    api = ApiConnector()
    _log.info(f"probing with day='{day}', docs → {DOCS_PATH}")

    DOCS_PATH.parent.mkdir(parents=True, exist_ok=True)

    doc_lines: list[str] = [
        f"# strategy_fm_* 底表体检报告",
        "",
        f"- 探测日期: **{day}**",
        f"- 探测脚本: [fm_etl_v3/scripts/probe_strategy_fm.py](../scripts/probe_strategy_fm.py)",
        f"- 生成方式: `SHOW FULL COLUMNS FROM <t>` + `SELECT COUNT/*... WHERE 日期列 = '{day}'`",
        "",
        "## 概览",
        "",
        "| # | 表 | 业务 | 日期列 | 预期行数 | 实测行数 | distinct store | distinct article | 结论 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]

    details: list[str] = []

    for idx, (table, date_col, biz, expected) in enumerate(TABLES, 1):
        _log.info(f"[{idx}/{len(TABLES)}] {table}")

        # 1. schema
        try:
            schema = probe_schema(api, table)
        except Exception as e:
            _log.error(f"schema error on {table}: {e}")
            schema = pd.DataFrame()
        schema_fields: list[str] = (
            schema["field"].astype(str).tolist() if "field" in schema.columns else []
        )

        # 2. counts
        counts = probe_counts(api, table, date_col, day, schema_fields)
        actual_rows = counts.get("total_rows")
        ds = counts.get("distinct_stores")
        da = counts.get("distinct_articles")
        store_col_used = counts.get("_store_col_name")
        article_col_used = counts.get("_article_col_name")

        # 3. sample
        sample = probe_sample(api, table, date_col, day, n=3)

        assessment = assess_row_count(table, actual_rows, expected)

        # 概览行
        def _fmt(v):
            if v is None: return "—"
            try: return f"{int(v):,}"
            except Exception: return str(v)

        doc_lines.append(
            "| {i} | `{t}` | {b} | `{c}` | {e} | {r} | {s} | {a} | {note} |".format(
                i=idx, t=table, b=biz, c=date_col,
                e=(f"{expected:,}" if expected is not None else "—"),
                r=_fmt(actual_rows),
                s=_fmt(ds),
                a=_fmt(da),
                note=assessment.replace("|", "\\|"),
            )
        )

        # 详情
        details.append(f"\n---\n\n## {idx}. `{table}` — {biz}\n")
        details.append(f"- 日期过滤列: `{date_col}`")
        details.append(f"- 行数预期（用户给定）: {'0（昨日无数据）' if expected == 0 else (f'{expected:,}' if expected is not None else '全量')}")
        details.append(f"- 行数实测（{day}）: {_fmt(actual_rows)}")
        if store_col_used:
            details.append(f"- 当日 distinct `{store_col_used}`: {_fmt(ds)}")
        if article_col_used:
            details.append(f"- 当日 distinct `{article_col_used}`: {_fmt(da)}")
        details.append(f"- 评估: **{assessment}**")
        details.append("")
        details.append(f"### 字段结构（{len(schema)} 列）")
        details.append("")
        details.append(render_schema_table(schema))
        details.append("")
        details.append(f"### 样本（前 3 行 × 前 15 列）")
        details.append("")
        details.append(render_sample(sample))
        details.append("")

    # 写文件
    doc_lines.append("")
    doc_lines.extend(details)
    DOCS_PATH.write_text("\n".join(doc_lines), encoding="utf-8")
    _log.info(f"report saved → {DOCS_PATH}")

    # 终端精简打印
    print("\n" + "=" * 72)
    print(f"探测完成: {len(TABLES)} 张表，详细报告 → {DOCS_PATH}")
    print("=" * 72)


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        day = sys.argv[1]
    else:
        # 当前上游 QDM 侧 strategy_fm_* 普遍只保留最新一天的 inc_day 分区，
        # 自动去探测 strategy_fm_sales_di 的 MAX(inc_day) 作为默认值
        try:
            api = ApiConnector()
            raw = api.query(
                "SELECT MAX(inc_day) AS d FROM strategy_fm_sales_di"
            ).iloc[0].to_dict()
            day = str(_pick(raw, "d"))[:10]
        except Exception:
            day = (date.today() - timedelta(days=2)).isoformat()
    main(day)
