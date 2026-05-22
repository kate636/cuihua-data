"""
查排骨(20003470)可能来自哪些parent SKU
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
    print("排骨(20003470)的来源parent分析（2026-04-23）")
    print("="*100)

    # 查所有sale_article_id = 20003470的记录（可能来自多个parent）
    sql = """
    SELECT
        article_id AS parent_article_id,
        article_name AS parent_name,
        sale_article_id AS sub_article_id,
        sale_article_name AS sub_name,
        sale_article_qty AS split_qty,
        sale_article_purchase_amt AS split_amt,
        avg_inbound_price AS parent_avg_price,
        day_clear
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND sale_article_id = '20003470'
    ORDER BY article_id
    """

    print("\n查询 purchase_di...")
    rows = query_sql(sql)

    if not rows:
        print("无数据")
        return

    print(f"共 {len(rows)} 条记录")

    # 汇总
    print("\n" + "="*120)
    print(f"\n{'ParentID':<12} {'Parent名称':<25} {'拆分量(kg)':<12} {'拆分额(元)':<12} {'Parent均价':<12} {'日清':<6}")
    print("-"*120)

    total_qty = 0
    total_amt = 0

    for r in rows:
        parent_id = r.get('parentArticleId', r.get('parent_article_id', ''))
        parent_name = str(r.get('parentName', r.get('parent_name', '')))[:25]
        qty = safe_float(r.get('splitQty', r.get('split_qty')))
        amt = safe_float(r.get('splitAmt', r.get('split_amt')))
        avg_price = safe_float(r.get('parentAvgPrice', r.get('parent_avg_price')))
        day_clear = r.get('dayClear', r.get('day_clear', ''))

        total_qty += qty
        total_amt += amt

        is_self = "✓ 标品" if parent_id == '20003470' else "BOM"
        print(f"{parent_id:<12} {parent_name:<25} {qty:<12.4f} {amt:<12.2f} {avg_price:<12.2f} {day_clear:<6} {is_self}")

    print("\n" + "="*120)
    print(f"\n汇总：")
    print(f"  总拆分量: {total_qty:.4f} kg")
    print(f"  总拆分额: {total_amt:.2f} 元")

    # 检查是否有标品（parent=sub）
    std_count = sum(1 for r in rows if r.get('parentArticleId', r.get('parent_article_id')) == '20003470')
    bom_count = len(rows) - std_count
    print(f"  标品(parent=sub): {std_count} 条")
    print(f"  BOM拆分(parent≠sub): {bom_count} 条")

    # 查销售量对比
    sql_sale = """
    SELECT
        abi_article_id,
        SUM(qty_spec) AS sale_qty,
        SUM(sales_amt) AS sale_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_sales_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND abi_article_id = '20003470'
    GROUP BY abi_article_id
    """
    print("\n查询 sales_di...")
    sale_rows = query_sql(sql_sale)
    if sale_rows:
        sale_qty = safe_float(sale_rows[0].get('saleQty', sale_rows[0].get('sale_qty')))
        sale_amt = safe_float(sale_rows[0].get('saleAmt', sale_rows[0].get('sale_amt')))
        print(f"  排骨实际销售量: {sale_qty:.4f} kg")
        print(f"  排骨实际销售额: {sale_amt:.2f} 元")
        print(f"  销售量 - 拆分量 = {sale_qty - total_qty:.4f} kg")

        if sale_qty > total_qty:
            print(f"\n  → 销售量 > 拆分量，说明排骨还来自其他渠道（如期初库存、或其他未记录的parent）")

if __name__ == "__main__":
    main()