"""
查找哪个表有期初库存数据
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
    body = {"apiId": API_ID, "paramMap": {"apiId": API_ID, "sql": sql}}
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
        raise RuntimeError(f"API error: {result.get('code')}, msg={result.get('msg')}")
    data = result.get("data")
    if isinstance(data, dict) and "pageData" in data:
        return data["pageData"]
    if isinstance(data, list):
        return data
    return []

def main():
    print("="*100)
    print("查找期初库存数据来源")
    print("="*100)

    # 1. 查所有 strategy_fm 表
    sql_tables = """
    SHOW TABLES FROM default_catalog.ads_business_analysis LIKE 'strategy_fm%'
    """
    print("\n所有 strategy_fm 表：")
    tables = query_sql(sql_tables)
    for t in tables:
        name = t.get('tablesInAdsBusinessAnalysis', t.get('tableName', t.get('name', '')))
        print(f"  {name}")

    # 2. 查看几个可能有库存数据的表

    # receive_sale_di - 进货销售表
    print("\n" + "="*100)
    print("\n查看 strategy_fm_receive_sale_di 表...")

    sql_receive = """
    SELECT *
    FROM default_catalog.ads_business_analysis.strategy_fm_receive_sale_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND sale_article_id = '20003470'
    """
    rows_receive = query_sql(sql_receive)
    print(f"共 {len(rows_receive)} 条记录")
    if rows_receive:
        print("\n第一条数据：")
        print(json.dumps(rows_receive[0], indent=2, ensure_ascii=False))

    # compose_di - 加工表
    print("\n" + "="*100)
    print("\n查看 strategy_fm_compose_di 表...")

    sql_compose = """
    SELECT *
    FROM default_catalog.ads_business_analysis.strategy_fm_compose_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND article_id = '20003470'
    """
    rows_compose = query_sql(sql_compose)
    print(f"共 {len(rows_compose)} 条记录")
    if rows_compose:
        print("\n第一条数据：")
        print(json.dumps(rows_compose[0], indent=2, ensure_ascii=False))

    # 3. 看看 purchase_di 的期初期末字段是否有非零数据（取任意几条）
    print("\n" + "="*100)
    print("\n查看 purchase_di 是否有非零期初库存数据...")

    sql_purchase_any = """
    SELECT
        sale_article_id,
        sale_article_name,
        init_stock_qty,
        init_stock_amt,
        end_stock_qty,
        end_stock_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND (init_stock_qty > 0 OR init_stock_amt > 0 OR end_stock_qty IS NOT NULL)
    LIMIT 20
    """
    rows_purchase = query_sql(sql_purchase_any)
    print(f"共 {len(rows_purchase)} 条有库存数据的记录")

    if rows_purchase:
        print(f"\n{'SubID':<12} {'名称':<25} {'期初量':<10} {'期初额':<10} {'期末量':<10} {'期末额':<10}")
        print("-"*80)
        for r in rows_purchase[:10]:
            sid = r.get('saleArticleId', r.get('sale_article_id', ''))
            name = str(r.get('saleArticleName', r.get('sale_article_name', '')))[:25]
            init_qty = r.get('initStockQty', r.get('init_stock_qty')) or 0
            init_amt = r.get('initStockAmt', r.get('init_stock_amt')) or 0
            end_qty = r.get('endStockQty', r.get('end_stock_qty'))
            end_amt = r.get('endStockAmt', r.get('end_stock_amt'))
            print(f"{sid:<12} {name:<25} {float(init_qty):<10.4f} {float(init_amt):<10.2f} {str(end_qty):<10} {str(end_amt):<10}")

if __name__ == "__main__":
    main()