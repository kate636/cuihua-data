"""
对比大白猪的官方拆分量 vs 实际销售量
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
    print("大白猪(20500351)：官方拆分量 vs 实际销售量（2026-04-23）")
    print("="*100)

    # 1. 取purchase_di的官方拆分数据
    sql_purchase = """
    SELECT
        sale_article_id AS sub_article_id,
        sale_article_name AS sub_name,
        sale_article_qty AS official_qty,
        sale_article_purchase_amt AS official_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND article_id = '20500351'
    ORDER BY sale_article_id
    """
    print("\n1. 查询 purchase_di（官方拆分）...")
    purchase_rows = query_sql(sql_purchase)
    print(f"   共 {len(purchase_rows)} 条")

    # 2. 取sales_di的实际销售数据（只取大白猪的sub）
    sub_ids = [r.get('subArticleId', r.get('sub_article_id')) for r in purchase_rows]

    sql_sales = """
    SELECT
        abi_article_id AS sub_article_id,
        SUM(qty_spec) AS sale_qty,
        SUM(sales_amt) AS sale_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_sales_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
    GROUP BY abi_article_id
    """
    print("\n2. 查询 sales_di（实际销售）...")
    sales_rows = query_sql(sql_sales)
    print(f"   共 {len(sales_rows)} 条")

    # 构建销售字典
    sale_dict = {}
    for r in sales_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        qty = safe_float(r.get('saleQty', r.get('sale_qty')))
        amt = safe_float(r.get('saleAmt', r.get('sale_amt')))
        sale_dict[sub_id] = {'qty': qty, 'amt': amt}

    # 3. 对比
    print("\n" + "="*140)
    print(f"\n{'SubID':<12} {'名称':<20} {'官方拆分量':<12} {'实际销售量':<12} {'差异':<10} {'官方拆分额':<12} {'实际销售额':<12}")
    print("-"*140)

    total_official_qty = 0
    total_sale_qty = 0
    matched = 0

    for r in purchase_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        sub_name = str(r.get('subName', r.get('sub_name', '')))[:20]
        official_qty = safe_float(r.get('officialQty', r.get('official_qty')))
        official_amt = safe_float(r.get('officialAmt', r.get('official_amt')))

        sale_data = sale_dict.get(sub_id, {'qty': 0, 'amt': 0})
        sale_qty = sale_data['qty']
        sale_amt = sale_data['amt']

        diff = sale_qty - official_qty
        total_official_qty += official_qty
        total_sale_qty += sale_qty
        if sale_qty > 0:
            matched += 1

        print(f"{sub_id:<12} {sub_name:<20} {official_qty:<12.4f} {sale_qty:<12.4f} {diff:<10.4f} {official_amt:<12.2f} {sale_amt:<12.2f}")

    print("\n" + "="*140)
    print("\n汇总：")
    print(f"  官方拆分量合计: {total_official_qty:.4f} kg")
    print(f"  实际销售量合计: {total_sale_qty:.4f} kg")
    print(f"  差异: {total_sale_qty - total_official_qty:.4f} kg")
    print(f"  有销售的sub数量: {matched} / {len(purchase_rows)} 个")

    if total_sale_qty > total_official_qty:
        print(f"\n  → 实际销售量 > 官方拆分量，说明有库存消耗")
        print(f"  → 官方拆分量代表【当日从parent进货拆出的量】")
        print(f"  → 实际销售可能来自：当日拆分 + 期初库存")
    elif total_sale_qty < total_official_qty:
        print(f"\n  → 实际销售量 < 官方拆分量，说明有期末库存增加")
        print(f"  → 官方拆分量代表【当日从parent进货拆出的量】")
        print(f"  → 未售部分进入期末库存")

if __name__ == "__main__":
    main()