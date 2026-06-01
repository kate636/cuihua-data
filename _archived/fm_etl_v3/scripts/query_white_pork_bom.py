"""
查看大白猪(parent=20500283)的官方BOM拆分数据
只取原始数据，不做计算
"""

import hashlib
import json
import random
import string
import time
import requests
import os
from dotenv import load_dotenv

load_dotenv('/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据/.env')

HOST = os.getenv('QDM_HOST', 'https://bdapp.qdama.cn')
API_ID = os.getenv('QDM_API_ID')
ACCESS_KEY = os.getenv('QDM_ACCESS_KEY')
SECRET_KEY = os.getenv('QDM_SECRET_KEY')
VERSION = os.getenv('QDM_VERSION', 'v1')

def generate_sign(timestamp, nonce, encrypt, body_str, access_key, secret_key, version):
    sign_params = {
        "AccessKey": access_key,
        "encrypt": encrypt,
        "nonce": nonce,
        "timestamp": timestamp,
        "version": version,
        "bodyStr": body_str,
    }
    keys = sorted(k for k, v in sign_params.items() if v not in (None, ""))
    param_str = "&".join(f"{k}={sign_params[k]}" for k in keys)
    param_str += f"&SecretKey={secret_key}"
    return hashlib.md5(param_str.encode("utf-8")).hexdigest().upper()

def query_sql(sql):
    body = {
        "apiId": API_ID,
        "paramMap": {"apiId": API_ID, "sql": sql},
    }
    body_str = json.dumps(body, ensure_ascii=False)

    nonce = "".join(random.choices(string.ascii_letters + string.digits, k=6))
    timestamp = int(time.time() * 1000)
    encrypt = 0

    sign = generate_sign(timestamp, nonce, encrypt, body_str, ACCESS_KEY, SECRET_KEY, VERSION)

    query_params = {
        "AccessKey": ACCESS_KEY,
        "timestamp": timestamp,
        "nonce": nonce,
        "encrypt": encrypt,
        "version": VERSION,
        "sign": sign,
    }
    url = f"{HOST}/api/v1/executeApi/{API_ID}?" + "&".join(f"{k}={v}" for k, v in query_params.items())

    headers = {"Content-Type": "application/json"}
    resp = requests.post(url, data=body_str.encode("utf-8"), headers=headers, timeout=600)
    resp.raise_for_status()
    result = resp.json()

    if result.get("code") != 0:
        raise RuntimeError(f"API error: code={result.get('code')}, msg={result.get('msg')}")

    data = result.get("data")
    if isinstance(data, dict) and "pageData" in data:
        return data["pageData"]
    if isinstance(data, list):
        return data
    return []

def safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def main():
    print("="*100)
    print("大白猪(20500351)官方BOM拆分数据（2026-04-23）")
    print("="*100)

    # 查询大白猪的purchase_di数据
    sql = """
    SELECT
        article_id AS parent_article_id,
        article_name AS parent_name,
        sale_article_id AS sub_article_id,
        sale_article_name AS sub_name,
        sale_article_qty AS official_qty,
        sale_article_purchase_amt AS official_amt,
        avg_inbound_price AS parent_avg_price,
        init_stock_qty,
        init_stock_amt,
        end_stock_qty,
        end_stock_amt,
        day_clear
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND article_id = '20500351'
    ORDER BY sale_article_id
    """

    print("\n查询 purchase_di...")
    rows = query_sql(sql)

    if not rows:
        print("无数据")
        return

    print(f"共 {len(rows)} 条记录")

    # 打印parent信息
    print("\n" + "="*120)
    print("【Parent信息】")
    parent_name = rows[0].get('parentName', rows[0].get('parent_name', ''))
    parent_avg_price = safe_float(rows[0].get('parentAvgPrice', rows[0].get('parent_avg_price')))
    print(f"  Parent ID: 20500283")
    print(f"  Parent名称: {parent_name}")
    print(f"  Parent均价: {parent_avg_price:.2f} 元/kg")

    # 计算汇总
    total_qty = sum(safe_float(r.get('officialQty', r.get('official_qty'))) for r in rows)
    total_amt = sum(safe_float(r.get('officialAmt', r.get('official_amt'))) for r in rows)
    print(f"  总拆分量: {total_qty:.3f} kg")
    print(f"  总拆分额: {total_amt:.2f} 元")

    # 打印所有sub
    print("\n" + "="*120)
    print("【官方拆分明细】")
    print(f"\n{'SubID':<12} {'名称':<20} {'拆分量(kg)':<12} {'拆分额(元)':<12} {'单位成本':<10} {'日清':<6} {'期初量':<10} {'期末量':<10}")
    print("-"*120)

    for r in rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id', ''))
        sub_name = str(r.get('subName', r.get('sub_name', '')))[:20]
        qty = safe_float(r.get('officialQty', r.get('official_qty')))
        amt = safe_float(r.get('officialAmt', r.get('official_amt')))
        day_clear = r.get('dayClear', r.get('day_clear', ''))
        init_qty = safe_float(r.get('initStockQty', r.get('init_stock_qty')))
        end_qty = safe_float(r.get('endStockQty', r.get('end_stock_qty')))

        unit_cost = amt / qty if qty > 0 else 0

        print(f"{sub_id:<12} {sub_name:<20} {qty:<12.4f} {amt:<12.4f} {unit_cost:<10.2f} {day_clear:<6} {init_qty:<10.4f} {end_qty if end_qty else 'NULL':<10}")

    # 统计BOM vs 标品
    print("\n" + "="*120)
    bom_count = sum(1 for r in rows if r.get('subArticleId', r.get('sub_article_id', '')) != '20500283')
    std_count = len(rows) - bom_count
    print(f"\n统计：")
    print(f"  BOM拆分商品(article_id ≠ sale_article_id): {bom_count} 条")
    print(f"  标品(article_id = sale_article_id): {std_count} 条")

    # 检查单位成本是否一致
    unit_costs = []
    for r in rows:
        qty = safe_float(r.get('officialQty', r.get('official_qty')))
        amt = safe_float(r.get('officialAmt', r.get('official_amt')))
        if qty > 0:
            unit_costs.append(amt / qty)

    if unit_costs:
        min_cost = min(unit_costs)
        max_cost = max(unit_costs)
        avg_cost = sum(unit_costs) / len(unit_costs)
        print(f"\n单位成本分析：")
        print(f"  最小: {min_cost:.2f} 元/kg")
        print(f"  最大: {max_cost:.2f} 元/kg")
        print(f"  平均: {avg_cost:.2f} 元/kg")
        print(f"  与parent均价差异: {avg_cost - parent_avg_price:.2f} 元/kg")

        if abs(max_cost - min_cost) < 0.5:
            print(f"\n  → 所有sub单位成本基本一致，接近parent均价（官方逻辑）")
        else:
            print(f"\n  → sub单位成本有差异，不是简单继承parent均价")

if __name__ == "__main__":
    main()