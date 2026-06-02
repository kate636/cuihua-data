#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成 v0.10 vs QDM 逐日 SKU 对比 HTML。
数据来源:
  - v0.10: 本地 DuckDB (t_calc_stock + t_calc_profit + t_calc_sku_cost)
  - QDM: API 直查 default_catalog...dal_transaction_chdj_store_sale_article_sale_info_di

用法: python3 generate_comparison_html.py [start_date] [end_date]
默认: 2026-05-01 ~ 2026-05-20
"""
import duckdb, json, sys, time
from pathlib import Path
from datetime import date, timedelta

DATA_DIR = Path(__file__).parent / "data"
DUCKDB_PATH = DATA_DIR / "fm.duckdb"
OUTPUT_DIR = DATA_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ════════════════════════ 日期工具 ════════════════════════
def date_range(start, end):
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    for n in range((e - s).days + 1):
        yield (s + timedelta(days=n)).isoformat()

# ════════════════════════ 1. 拉取 QDM 数据 ════════════════════════
def fetch_qdm(start_date, end_date):
    """逐天从 QDM API 拉取 dal_transaction_* 表数据"""
    from fmetl.connectors.api_connector import ApiConnector
    from fmetl.config.settings import get_settings

    cfg = get_settings()
    api = ApiConnector(cfg)

    qdm_fields = [
        'store_id', 'article_id', 'business_date', 'day_clear',
        'sale_amt', 'sale_qty',
        'receive_amt', 'receive_qty',
        'compose_in_amt', 'compose_in_qty',
        'compose_out_amt', 'compose_out_qty',
        'init_stock_amt', 'init_stock_qty',
        'end_stock_amt', 'end_stock_qty',
        'know_lost_amt', 'know_lost_qty',
        'unknow_lost_amt', 'unknow_lost_qty',
        'profit_amt',
    ]
    cols = ', '.join(qdm_fields)

    all_dfs = []
    dates = list(date_range(start_date, end_date))
    total_days = len(dates)

    print(f"拉取 QDM: {start_date} ~ {end_date} ({total_days} 天)")

    for i, d in enumerate(dates):
        sql = f"""
        SELECT {cols}
        FROM default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di
        WHERE inc_day = '{d}'
        """
        try:
            df = api.query(sql)
            all_dfs.append(df)
            print(f"  [{i+1}/{total_days}] {d}: {len(df)} rows")
        except Exception as e:
            print(f"  [{i+1}/{total_days}] {d}: FAILED - {e}")

    import pandas as pd
    if not all_dfs:
        print("ERROR: 所有 QDM 查询均失败")
        return None

    qdm = pd.concat(all_dfs, ignore_index=True)
    print(f"QDM 合计: {len(qdm)} rows, {qdm['business_date'].nunique()} days")
    return qdm


# ════════════════════════ 2. 提取 v0.10 本地数据 ════════════════════════
def fetch_fm(start_date, end_date):
    """从本地 DuckDB 提取 v0.10 计算层数据 (去重)"""
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    # t_calc_stock (去重)
    stock = conn.execute(f"""
        SELECT * FROM (
            SELECT s.*, ROW_NUMBER() OVER (
                PARTITION BY store_id, business_date, article_id, day_clear
                ORDER BY effective_unit_cost DESC
            ) AS rn
            FROM t_calc_stock s
            WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
        ) WHERE rn = 1
    """).df()

    # t_calc_profit (去重)
    profit = conn.execute(f"""
        SELECT * FROM (
            SELECT p.*, ROW_NUMBER() OVER (
                PARTITION BY store_id, business_date, article_id, day_clear
                ORDER BY profit_amt DESC
            ) AS rn
            FROM t_calc_profit p
            WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
        ) WHERE rn = 1
    """).df()

    # dim_goods
    goods = conn.execute("""
        SELECT DISTINCT article_id, article_name,
            category_level1_description AS cat_l1,
            category_level2_description AS cat_l2,
            category_level3_description AS cat_l3
        FROM dim_goods
    """).df()

    # BOM 关系
    bom = conn.execute(f"""
        SELECT DISTINCT article_id AS parent_id, sale_article_id AS sub_id
        FROM atomic_receive_sale
        WHERE article_id != sale_article_id
          AND business_date BETWEEN '{start_date}' AND '{end_date}'
    """).df()
    parent_set = set(bom['parent_id'].unique())
    sub_set = set(bom['sub_id'].unique())

    # 加工品
    comp = conn.execute(f"""
        SELECT DISTINCT article_id FROM atomic_compose
        WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
          AND (ABS(compose_in_qty) > 0.001 OR ABS(compose_out_qty) > 0.001)
    """).df()
    comp_set = set(comp['article_id'].unique())

    conn.close()

    # 合并 stock + profit
    keys = ['store_id', 'business_date', 'article_id', 'day_clear']
    profit_cols = keys + ['profit_amt']
    df = stock.merge(profit[profit_cols], on=keys, how='left')

    # 合并 goods
    df = df.merge(goods, on='article_id', how='left')
    for c in ['article_name', 'cat_l1', 'cat_l2', 'cat_l3']:
        df[c] = df[c].fillna('')

    # 类别重映射
    def remap(row):
        c1, c2, c3 = str(row['cat_l1']), str(row['cat_l2']), str(row['cat_l3'])
        if c2 in ('蛋类', '烘焙类'): return c2
        if c2 in ('冷藏奶制品类', '饮料类'): return '乳制品及水饮类'
        if c1 == '肉禽蛋类' and c2 != '蛋类': return '肉禽类'
        if c3.endswith('熟食'): return '熟食类'
        if c1 in ('冷藏及加工类', '预制菜'): return '冷藏加工及预制菜类'
        return c1
    df['cat'] = df.apply(remap, axis=1)

    # tag
    def get_tag(aid):
        tags = []
        if aid in parent_set: tags.append('订购码')
        if aid in sub_set: tags.append('销售码')
        if aid in comp_set: tags.append('加工')
        return ','.join(tags)
    df['tag'] = df['article_id'].apply(get_tag)
    df['cls'] = df['tag'].apply(
        lambda t: ('bp' if '订购码' in t else '') + (' bs' if '销售码' in t else '') + (' cp' if '加工' in t else '')
    ).str.strip()

    print(f"v0.10: {len(df)} rows, {df['business_date'].nunique()} days, {df['article_id'].nunique()} SKUs")
    return df


# ════════════════════════ 3. 合并 v0.10 + QDM → JSON ════════════════════════
def build_comparison(fm_df, qdm_df):
    """合并 v0.10 和 QDM 数据, 产出对比 JSON"""
    import pandas as pd

    # 统一 day_clear 类型
    fm_df['day_clear'] = fm_df['day_clear'].astype(str)
    if qdm_df is not None:
        qdm_df['day_clear'] = qdm_df['day_clear'].astype(str)

    # 合并键
    keys = ['store_id', 'business_date', 'article_id', 'day_clear']

    # 字段映射: 对比名 → (fm_amt, fm_qty, qdm_amt, qdm_qty)
    field_map = [
        ('销售额',   'sale_amt', 'sale_qty', 'sale_amt', 'sale_qty'),
        ('进货',     'receive_amt', 'receive_qty', 'receive_amt', 'receive_qty'),
        ('BOM入',    'bom_in_amt', 'bom_in_qty', None, None),
        ('BOM出',    'bom_out_amt', 'bom_out_qty', None, None),
        ('加工入',   'compose_in_amt', 'compose_in_qty', 'compose_in_amt', 'compose_in_qty'),
        ('加工出',   'compose_out_amt', 'compose_out_qty', 'compose_out_amt', 'compose_out_qty'),
        ('库存转出', 'stock_transfer_out_amt', 'stock_transfer_out_qty', None, None),
        ('库存转入', 'stock_transfer_in_amt', 'stock_transfer_in_qty', None, None),
        ('期初库存', 'init_stock_amt', 'init_stock_qty', 'init_stock_amt', 'init_stock_qty'),
        ('期末库存', 'end_stock_amt', 'end_stock_qty', 'end_stock_amt', 'end_stock_qty'),
        ('已知损耗', 'know_lost_amt', 'know_lost_qty', 'know_lost_amt', 'know_lost_qty'),
        ('未知损耗', 'unknow_lost_amt', 'unknow_lost_qty', 'unknow_lost_amt', 'unknow_lost_qty'),
        ('门店毛利', 'profit_amt', None, 'profit_amt', None),
    ]

    rows = []
    dates = sorted(fm_df['business_date'].unique())

    for d in dates:
        fm_day = fm_df[fm_df['business_date'] == d]
        qdm_day = qdm_df[qdm_df['business_date'] == d] if qdm_df is not None else None

        # 合并 v0.10 和 QDM
        if qdm_day is not None and len(qdm_day) > 0:
            merged = fm_day.merge(qdm_day, on=keys, how='outer', suffixes=('_v0.10', '_qdm'))
        else:
            merged = fm_day.copy()
            for c in fm_day.columns:
                if c not in keys:
                    merged[f'{c}_v0.10'] = merged[c]

        # 对每个 SKU (取非日清行优先)
        for aid, grp in merged.groupby('article_id'):
            # 优先选 day_clear='1' 的行, 如果没有则取第一行
            r1 = grp[grp['day_clear'] == '1']
            base = r1.iloc[0] if len(r1) > 0 else grp.iloc[0]

            row = {
                'date': str(d),
                'id': str(aid),
                'nm': str(base.get('article_name_v0.10', base.get('article_name', ''))),
                'cat': str(base.get('cat_v0.10', base.get('cat', '?'))),
                'tag': str(base.get('tag_v0.10', base.get('tag', ''))),
                'cls': str(base.get('cls_v0.10', base.get('cls', ''))),
                'euc': round(float(grp['effective_unit_cost_v0.10'].mean()) if 'effective_unit_cost_v0.10' in grp.columns
                       else float(grp['effective_unit_cost'].mean()) if 'effective_unit_cost' in grp.columns
                       else 0, 4),
            }

            for name, fm_a, fm_q, qdm_a, qdm_q in field_map:
                # merge 后 v0.10列带 _v0.10 后缀, QDM列带 _qdm 后缀
                vA_col = f'{fm_a}_v0.10' if fm_a else None
                vQ_col = f'{fm_q}_v0.10' if fm_q else None
                qA_col = f'{qdm_a}_qdm' if qdm_a else None
                qQ_col = f'{qdm_q}_qdm' if qdm_q else None

                vA = float(grp[vA_col].sum()) if vA_col and vA_col in grp.columns else 0.0
                vQ = float(grp[vQ_col].sum()) if vQ_col and vQ_col in grp.columns else 0.0
                qA = float(grp[qA_col].sum()) if qA_col and qA_col in grp.columns else None
                qQ = float(grp[qQ_col].sum()) if qQ_col and qQ_col in grp.columns else None

                dA = round(vA - qA, 4) if (qA is not None) else None
                dQ = round(vQ - qQ, 4) if (qQ is not None and fm_q) else None

                row[name] = {
                    'vA': round(vA, 4), 'vQ': round(vQ, 4),
                    'qA': round(qA, 4) if qA is not None else None,
                    'qQ': round(qQ, 4) if qQ is not None else None,
                    'dA': dA, 'dQ': dQ,
                }
            rows.append(row)

    print(f"对比记录: {len(rows)} 条")
    return rows, [str(d) for d in dates]


# ════════════════════════ 4. HTML 模板 ════════════════════════
HTML_TEMPLATE = r'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>v0.10 vs QDM 每日对比</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,"Microsoft YaHei",sans-serif;font-size:12px;background:#f5f5f5;color:#333}
.hdr{background:linear-gradient(135deg,#1a1a2e,#16213e);color:#fff;padding:8px 14px;display:flex;align-items:center;gap:10px}
.hdr h1{font-size:14px}.hdr s{font-size:9px;opacity:.4}
.fm{margin:6px 14px;padding:6px 12px;background:#fffbe6;border:1px solid #ffe58f;border-radius:5px;font-size:10px;text-align:center;font-family:"SF Mono",Menlo,monospace}
.fm .p{color:#cf1322;font-weight:700}.fm .m{color:#1890ff;font-weight:700}.fm .e{color:#389e0d;font-weight:700}.fm .x{color:#999}
.leg{margin:4px 14px;display:flex;gap:10px;font-size:9px;flex-wrap:wrap}
.leg span{padding:1px 6px;border-radius:2px;border:1px solid #ddd}
.leg .lbp{background:#e3f2fd;border-color:#90caf9}.leg .lbs{background:#fff3e0;border-color:#ffcc80}.leg .lcp{background:#e8f5e9;border-color:#a5d6a7}
.bar{background:#fff;margin:0 14px;padding:5px 10px;display:flex;gap:6px;align-items:center;border:1px solid #e8e8e8;border-radius:5px 5px 0 0;flex-wrap:wrap;font-size:10px}
.bar select,.bar input{padding:3px 5px;border:1px solid #d9d9d9;border-radius:3px;font-size:10px}
.bar label{color:#888;font-size:10px}#info{margin-left:auto;color:#888;font-size:10px}
.tbl-outer{margin:0 14px 14px;border:1px solid #e8e8e8;border-top:none;border-radius:0 0 5px 5px;overflow-x:auto;background:#fff}
.tbl-inner{min-width:2600px}
th{background:#1a1a2e;color:#fff;padding:2px 3px;text-align:right;white-space:nowrap;font-weight:500;font-size:7px}
th.stk{text-align:left;position:sticky;z-index:3;background:#16213e}
th.c1{left:0;min-width:82px;max-width:82px;font-size:10px}
th.c2{left:82px;min-width:88px;max-width:88px;font-size:10px}
th.c3{left:170px;min-width:58px;max-width:58px;font-size:10px}
th.tc{text-align:center;position:sticky;left:228px;z-index:3;background:#333;font-size:7px;min-width:42px;max-width:42px}
th.fc{background:#0f3460;text-align:center;font-size:7px;font-weight:400}
th.rc{background:#444;text-align:center;font-size:7px;font-weight:400}
td{padding:2px 3px;text-align:right;border-bottom:1px solid #f5f5f5;font-size:9px;white-space:nowrap}
td.stk{text-align:left;font-weight:500;position:sticky;background:#fff;z-index:1;font-size:10px}
td.c1{left:0;min-width:82px;max-width:82px}td.c2{left:82px;min-width:88px;max-width:88px}td.c3{left:170px;min-width:58px;max-width:58px;font-size:9px}
td.tc{text-align:center;font-size:7px;font-weight:600;position:sticky;left:228px;background:#fff;z-index:1;min-width:42px;max-width:42px}
tr:hover td{background:#e6f7ff!important}tr:hover td.stk,tr:hover td.tc{background:#e6f7ff!important}
tr.tot td{background:#fafafa!important;border-top:2px solid #1a1a2e;border-bottom:2px solid #1a1a2e;font-weight:700;font-size:9px}
tr.tot td.stk,tr.tot td.tc{background:#fafafa!important}
.df{font-weight:700}.df.p{color:#cf1322}.df.n{color:#389e0d}.df.o{color:#bbb}.q{color:#999}
.cards{display:flex;gap:6px;flex-wrap:wrap;margin:0 14px 4px}
.cd{background:#fff;padding:4px 8px;border-radius:4px;border:1px solid #e8e8e8;text-align:center;min-width:55px}
.cd .vl{font-size:12px;font-weight:700}.cd .lb{font-size:7px;color:#999}
.date-warn{background:#fff2f0;border:1px solid #ffccc7;padding:4px 12px;margin:4px 14px;border-radius:4px;font-size:10px;color:#cf1322}
</style>
</head>
<body>
<div class="hdr">
  <h1>SKU全字段对比 v0.10 vs QDM</h1>
  <s id="dateLabel">全部日期(日均)</s>
</div>
<div class="fm">
  <span class="e">门店毛利</span> = <span class="p">+销售额</span> <span class="m">&minus;进货</span> <span class="m">&minus;BOM入</span> <span class="p">+BOM出</span> <span class="m">&minus;加工入</span> <span class="p">+加工出</span> <span class="p">+期末库存</span> <span class="m">&minus;期初库存</span>
  <span class="x">[v0.10=计算层 | Q=QDM直查 dal_transaction_* 表]</span>
</div>
<div class="leg">
  <span class="lbp">订购码</span><span class="lbs">销售码</span><span class="lcp">加工</span>
  <b>单日</b>=精确对比 &nbsp; <b>全部/区间</b>=日均(合计/天数)
</div>

<div class="bar">
  <label>&#x1F4C5; 单日</label>
  <select id="datePick" onchange="onDateChange()"><option value="">全部(日均)</option></select>
  <label>从</label><input type="date" id="dateFrom" onchange="onRangeChange()" style="width:115px">
  <label>到</label><input type="date" id="dateTo" onchange="onRangeChange()" style="width:115px">
  <label id="avgLabel" style="color:#1890ff;font-weight:700;font-size:11px"></label>
  <label>分类</label><select id="cf" onchange="f()"><option value="">全部</option>__CATEGORIES__</select>
  <label>搜索</label><input id="kw" placeholder="编码/名称..." oninput="f()" style="width:100px">
  <label>标记</label><select id="tf" onchange="f()"><option value="">全部</option><option value="bp">订购码</option><option value="bs">销售码</option><option value="cp">加工</option></select>
  <label>毛利差&ge;</label><select id="th" onchange="f()"><option value="0">全部</option><option value="5">5</option><option value="10">10</option><option value="50">50</option></select>
  <label>排序</label><select id="so" onchange="f()"><option value="d">毛利差&darr;</option><option value="u">未知损耗差&darr;</option><option value="p">毛利&darr;</option><option value="c">分类</option></select>
  <span id="info"></span>
</div>
<div class="date-warn" id="dateWarn" style="display:none"></div>
<div class="cards" id="cards"></div>
<div class="tbl-outer"><div class="tbl-inner"><table><thead>
<tr><th class="stk c1">编码</th><th class="stk c2">名称</th><th class="stk c3">分类</th><th class="tc">标记</th><th class="fc" colspan="6">+ 销售额</th><th class="fc" colspan="6">&minus; 进货</th><th class="fc" colspan="2">&minus; BOM入</th><th class="fc" colspan="2">+ BOM出</th><th class="fc" colspan="6">&minus; 加工入</th><th class="fc" colspan="6">+ 加工出</th><th class="rc" colspan="2"> 库存转出</th><th class="rc" colspan="2"> 库存转入</th><th class="fc" colspan="6">&minus; 期初库存</th><th class="fc" colspan="6">+ 期末库存</th><th class="rc" colspan="6"> 已知损耗</th><th class="rc" colspan="6"> 未知损耗</th><th class="fc" colspan="3">= 门店毛利</th><th class="fc">euc</th></tr>
<tr><th class="stk c1"></th><th class="stk c2"></th><th class="stk c3"></th><th class="tc"></th><th>v0.10额</th><th>Q额</th><th>&Delta;额</th><th>v0.10量</th><th>Q量</th><th>&Delta;量</th><th>v0.10额</th><th>Q额</th><th>&Delta;额</th><th>v0.10量</th><th>Q量</th><th>&Delta;量</th><th>v0.10额</th><th>v0.10量</th><th>v0.10额</th><th>v0.10量</th><th>v0.10额</th><th>Q额</th><th>&Delta;额</th><th>v0.10量</th><th>Q量</th><th>&Delta;量</th><th>v0.10额</th><th>Q额</th><th>&Delta;额</th><th>v0.10量</th><th>Q量</th><th>&Delta;量</th><th>v0.10额</th><th>v0.10量</th><th>v0.10额</th><th>v0.10量</th><th>v0.10额</th><th>Q额</th><th>&Delta;额</th><th>v0.10量</th><th>Q量</th><th>&Delta;量</th><th>v0.10额</th><th>Q额</th><th>&Delta;额</th><th>v0.10量</th><th>Q量</th><th>&Delta;量</th><th>v0.10额</th><th>Q额</th><th>&Delta;额</th><th>v0.10量</th><th>Q量</th><th>&Delta;量</th><th>v0.10额</th><th>Q额</th><th>&Delta;额</th><th>v0.10量</th><th>Q量</th><th>&Delta;量</th><th>v0.10额</th><th>Q额</th><th>&Delta;额</th><th>v0.10</th></tr>
</thead><tbody id="body"></tbody></table></div></div>

<script>
var ALL_DATES = __DATES__;
var RAW = __DATA__;

var dateIdx = {};
RAW.forEach(function(r) {
  var d = r.date;
  if (!dateIdx[d]) dateIdx[d] = [];
  dateIdx[d].push(r);
});

var D = [];
var dayCount = 0;
var currentMode = 'all';
var filteredIds = null;

// 列定义: [名称, 符号, 有数量列?, 对比QDM?]
var P = [
  ["销售额","+",1,1],
  ["进货","-",1,1],
  ["BOM入","-",1,0],
  ["BOM出","+",1,0],
  ["加工入","-",1,1],
  ["加工出","+",1,1],
  ["库存转出","+",1,0],
  ["库存转入","+",1,0],
  ["期初库存","-",1,1],
  ["期末库存","+",1,1],
  ["已知损耗","-",1,1],
  ["未知损耗","-",1,1],
  ["门店毛利","=",0,1]
];

(function init() {
  var sel = document.getElementById('datePick');
  ALL_DATES.forEach(function(d) {
    var opt = document.createElement('option');
    opt.value = d; opt.textContent = d;
    sel.appendChild(opt);
  });
  selectAllDates();
})();

function selectAllDates() {
  currentMode = 'all';
  document.getElementById('datePick').value = '';
  document.getElementById('dateLabel').textContent = '全部日期(日均)';
  document.getElementById('avgLabel').textContent = '日均模式';
  D = aggregateBySku(RAW);
  dayCount = ALL_DATES.length;
  document.getElementById('dateWarn').style.display = 'none';
  f();
}

function onDateChange() {
  var v = document.getElementById('datePick').value;
  if (!v) { selectAllDates(); return; }
  currentMode = 'single';
  document.getElementById('dateLabel').textContent = v;
  document.getElementById('avgLabel').textContent = '';
  D = dateIdx[v] || [];
  dayCount = 1;
  var warn = document.getElementById('dateWarn');
  warn.style.display = D.length === 0 ? '' : 'none';
  if (D.length === 0) warn.textContent = '⚠ ' + v + ' 无数据';
  f();
}

function onRangeChange() {
  var from = document.getElementById('dateFrom').value;
  var to = document.getElementById('dateTo').value;
  if (!from || !to) return;
  currentMode = 'range';
  document.getElementById('datePick').value = '';
  document.getElementById('dateLabel').textContent = from + ' ~ ' + to + ' (日均)';
  var inRange = [];
  ALL_DATES.forEach(function(d) {
    if (d >= from && d <= to && dateIdx[d]) inRange = inRange.concat(dateIdx[d]);
  });
  D = aggregateBySku(inRange);
  var rangeDays = ALL_DATES.filter(function(d) { return d >= from && d <= to; }).length;
  dayCount = rangeDays;
  document.getElementById('avgLabel').textContent = rangeDays + '天日均';
  document.getElementById('dateWarn').style.display = rangeDays === 0 ? '' : 'none';
  f();
}

function aggregateBySku(rows) {
  var map = {};
  rows.forEach(function(r) {
    var key = r.id;
    if (!map[key]) {
      map[key] = { id: r.id, nm: r.nm, cat: r.cat, tag: r.tag, cls: r.cls||'', euc: 0, _count: 0 };
    }
    map[key]._count++;
    map[key].euc += r.euc || 0;
  });

  var F = ['销售额','进货','BOM入','BOM出','加工入','加工出','库存转出','库存转入','期初库存','期末库存','已知损耗','未知损耗','门店毛利'];
  var result = [];
  Object.keys(map).forEach(function(key) {
    var agg = map[key];
    var n = agg._count;
    agg.euc = n > 0 ? agg.euc / n : 0;
    var skuRows = rows.filter(function(r) { return r.id === key; });

    F.forEach(function(f) {
      var vAs=0, vQs=0, qAs=0, qQs=0, qAc=0, qQc=0;
      skuRows.forEach(function(row) {
        var d = row[f];
        if (d) {
          vAs += d.vA||0; vQs += d.vQ||0;
          if (d.qA != null) { qAs += d.qA; qAc++; }
          if (d.qQ != null) { qQs += d.qQ; qQc++; }
        }
      });
      agg[f] = {
        vA: n>0?+(vAs/n).toFixed(4):0, vQ: n>0?+(vQs/n).toFixed(4):0,
        qA: qAc>0?+(qAs/n).toFixed(4):null, qQ: qQc>0?+(qQs/n).toFixed(4):null,
        dA: qAc>0?+((vAs-qAs)/n).toFixed(4):null, dQ: qQc>0?+((vQs-qQs)/n).toFixed(4):null
      };
    });
    result.push(agg);
  });
  return result;
}

function rr(r) {
  var h = '<tr class="'+(r.cls||'')+'" id="r_'+r.id+'" data-row="'+r.id+'" style="cursor:pointer">';
  h += '<td class="stk c1">'+r.id+'</td><td class="stk c2">'+(r.nm||'')+'</td><td class="stk c3" style="font-size:9px">'+r.cat+'</td><td class="tc">'+(r.tag||'')+'</td>';
  P.forEach(function(p) {
    var n=p[0], hasQty=p[2], hasQDM=p[3], d=r[n];
    if(!d){h+=hasQDM?(hasQty?'<td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>':'<td>-</td><td>-</td><td>-</td>'):(hasQty?'<td>-</td><td>-</td>':'<td>-</td>');return;}
    var da=d.dA,dc=da==null?'o':(Math.abs(da)<0.001?'o':(da>0?'p':'n'));
    var dq=d.dQ,dcq=dq==null?'o':(Math.abs(dq||0)<0.001?'o':((dq||0)>0?'p':'n'));
    if(hasQDM){
      if(hasQty) h+='<td>'+d.vA.toFixed(2)+'</td><td class="q">'+(d.qA!=null?d.qA.toFixed(2):'-')+'</td><td class="df '+dc+'">'+(da!=null?da.toFixed(2):'-')+'</td><td>'+(d.vQ!=null?d.vQ.toFixed(2):'-')+'</td><td class="q">'+(d.qQ!=null?d.qQ.toFixed(2):'-')+'</td><td class="df '+dcq+'">'+(dq!=null?dq.toFixed(2):'-')+'</td>';
      else h+='<td>'+d.vA.toFixed(2)+'</td><td class="q">'+(d.qA!=null?d.qA.toFixed(2):'-')+'</td><td class="df '+dc+'">'+(da!=null?da.toFixed(2):'-')+'</td>';
    }else{
      h+='<td>'+d.vA.toFixed(2)+'</td>'+(hasQty?'<td>'+(d.vQ!=null?d.vQ.toFixed(2):'-')+'</td>':'');
    }
  });
  h+='<td>'+((r.euc||0).toFixed(4))+'</td></tr>';
  return h;
}

function f() {
  var cf=document.getElementById('cf').value;
  var tf=document.getElementById('tf').value;
  var kw=(document.getElementById('kw').value||'').toLowerCase();
  var th=parseFloat(document.getElementById('th').value)||0;
  var so=document.getElementById('so').value;

  var fd=D.slice();
  if(cf) fd=fd.filter(function(r){return r.cat===cf;});
  if(tf) fd=fd.filter(function(r){return (r.cls||'').indexOf(tf)>=0;});
  if(kw) fd=fd.filter(function(r){return r.id.indexOf(kw)>=0||(r.nm||'').toLowerCase().indexOf(kw)>=0;});
  if(th>0) fd=fd.filter(function(r){return r['门店毛利']&&Math.abs(r['门店毛利'].dA)>th;});

  if(so==='u') fd.sort(function(a,b){return Math.abs(b['未知损耗']&&b['未知损耗'].dA||0)-Math.abs(a['未知损耗']&&a['未知损耗'].dA||0);});
  else if(so==='d') fd.sort(function(a,b){return Math.abs(b['门店毛利']&&b['门店毛利'].dA||0)-Math.abs(a['门店毛利']&&a['门店毛利'].dA||0);});
  else if(so==='p') fd.sort(function(a,b){return b['门店毛利'].vA-a['门店毛利'].vA;});
  else fd.sort(function(a,b){return a.cat.localeCompare(b.cat)||b['门店毛利'].vA-a['门店毛利'].vA;});

  var tot={}; P.forEach(function(p){var n=p[0];tot[n]={s4A:0,s4Q:0,sQA:0,sQQ:0};});
  fd.forEach(function(r){P.forEach(function(p){var n=p[0],d=r[n];if(d){tot[n].s4A+=d.vA||0;tot[n].s4Q+=d.vQ||0;tot[n].sQA+=d.qA||0;tot[n].sQQ+=d.qQ||0;}});});

  var tr='<tr class="tot"><td class="stk c1">合计</td><td class="stk c2">'+fd.length+'SKU</td><td class="stk c3"></td><td class="tc"></td>';
  P.forEach(function(p){
    var n=p[0],hasQty=p[2],hasQDM=p[3],t=tot[n],dA=t.s4A-t.sQA,dQ=t.s4Q-t.sQQ,ca=Math.abs(dA)<0.001?'o':(dA>0?'p':'n'),cq=Math.abs(dQ)<0.001?'o':(dQ>0?'p':'n');
    if(hasQDM){
      if(hasQty) tr+='<td>'+t.s4A.toFixed(2)+'</td><td class="q">'+t.sQA.toFixed(2)+'</td><td class="df '+ca+'">'+dA.toFixed(2)+'</td><td>'+t.s4Q.toFixed(2)+'</td><td class="q">'+t.sQQ.toFixed(2)+'</td><td class="df '+cq+'">'+dQ.toFixed(2)+'</td>';
      else tr+='<td>'+t.s4A.toFixed(2)+'</td><td class="q">'+t.sQA.toFixed(2)+'</td><td class="df '+ca+'">'+dA.toFixed(2)+'</td>';
    }else{tr+='<td>'+t.s4A.toFixed(2)+'</td>'+(hasQty?'<td>'+t.s4Q.toFixed(2)+'</td>':'');}
  });
  tr+='<td></td></tr>';
  document.getElementById('body').innerHTML=tr+fd.map(rr).join('');

  var ml=currentMode==='all'?'日均':(currentMode==='range'?dayCount+'天日均':'单日');
  document.getElementById('info').textContent=fd.length+' SKU ('+ml+')';

  var tp=tot['门店毛利'].s4A-tot['门店毛利'].sQA,tu=tot['未知损耗'].s4A-tot['未知损耗'].sQA,te=tot['期末库存'].s4A-tot['期末库存'].sQA;
  document.getElementById('cards').innerHTML=
    '<div class="cd"><div class="vl">'+tot['销售额'].s4A.toFixed(0)+'</div><div class="lb">v0.10&S卖'+'</div></div>'+
    '<div class="cd"><div class="vl" style="color:'+(Math.abs(tp)>100?(tp>0?'#cf1322':'#389e0d'):'#333')+'">'+(tp>0?'+':'')+tp.toFixed(1)+'</div><div class="lb">S毛利差 v0.10-Q'+'</div></div>'+
    '<div class="cd"><div class="vl" style="color:'+(Math.abs(tu)>100?'#cf1322':'#333')+'">'+(tu>0?'+':'')+tu.toFixed(1)+'</div><div class="lb">S未知损耗差'+'</div></div>'+
    '<div class="cd"><div class="vl" style="color:'+(Math.abs(te)>1000?'#cf1322':'#333')+'">'+(te>0?'+':'')+te.toFixed(1)+'</div><div class="lb">S期末库存差'+'</div></div>'+
    '<div class="cd"><div class="vl">'+dayCount+'</div><div class="lb">天数</div></div>';
}

document.addEventListener('click',function(e){
  var tr=e.target.closest('tr');
  if(!tr||!tr.dataset.row)return;
  var aid=tr.dataset.row;
  if(filteredIds){filteredIds=null;f();return;}
  filteredIds=new Set([aid]);f();
});
</script>
</body></html>'''


# ════════════════════════ main ════════════════════════
if __name__ == '__main__':
    import sys
    start_date = sys.argv[1] if len(sys.argv) > 1 else '2026-05-01'
    end_date = sys.argv[2] if len(sys.argv) > 2 else '2026-05-20'

    print("=" * 60)
    print(f"v0.10 vs QDM 对比报告: {start_date} ~ {end_date}")
    print("=" * 60)

    # 1. 拉取 QDM
    qdm = fetch_qdm(start_date, end_date)

    # 2. 提取 FM
    fm = fetch_fm(start_date, end_date)

    # 3. 合并对比
    rows, dates = build_comparison(fm, qdm)

    # 4. 生成 HTML
    dates_json = json.dumps(dates)
    data_json = json.dumps(rows, ensure_ascii=False)
    cats = sorted(set(r['cat'] for r in rows))
    cat_options = '\n'.join(f'<option value="{c}">{c}</option>' for c in cats)

    html = HTML_TEMPLATE.replace('__DATES__', dates_json)
    html = html.replace('__DATA__', data_json)
    html = html.replace('__CATEGORIES__', cat_options)

    outpath = OUTPUT_DIR / f'v0.10_vs_QDM_{start_date}_{end_date}.html'
    outpath.write_text(html, encoding='utf-8')
    mb = outpath.stat().st_size / (1024 * 1024)
    print(f"\n完成! ({mb:.1f} MB)")
    print(f"  open {outpath}")
