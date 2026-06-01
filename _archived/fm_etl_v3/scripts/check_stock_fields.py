"""
直接查看 purchase_di 表的库存字段原始数据
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
    print("查看 purchase_di 表的库存字段原始数据（2026-04-23）")
    print("="*100)

    # 查排骨的所有记录（含库存字段）
    sql = """
    SELECT
        article_id AS parent_id,
        sale_article_id AS sub_id,
        sale_article_name AS sub_name,
        sale_article_qty AS split_qty,
        sale_article_purchase_amt AS split_amt,
        init_stock_qty,
        init_stock_amt,
        end_stock_qty,
        end_stock_amt,
        avg_inbound_price
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND sale_article_id = '20003470'
    """
    print("\n查询排骨(20003470)的库存数据...")
    rows = query_sql(sql)

    print(f"\n共 {len(rows)} 条记录")
    print(f"\n{'ParentID':<12} {'SubID':<12} {'拆分量':<10} {'拆分额':<10} {'期初量':<10} {'期初额':<10} {'期末量':<10} {'期末额':<10}")
    print("-"*100)

    for r in rows:
        pid = r.get('parentId', r.get('parent_id', ''))
        sid = r.get('subId', r.get('sub_id', ''))
        name = str(r.get('subName', r.get('sub_name', '')))[:15]
        split_qty = float(r.get('splitQty', r.get('split_qty', 0)) or 0)
        split_amt = float(r.get('splitAmt', r.get('split_amt', 0)) or 0)
        init_qty = r.get('initStockQty', r.get('init_stock_qty'))
        init_amt = r.get('initStockAmt', r.get('init_stock_amt'))
        end_qty = r.get('endStockQty', r.get('end_stock_qty'))
        end_amt = r.get('endStockAmt', r.get('end_stock_amt'))

        print(f"{pid:<12} {sid:<12} {split_qty:<10.4f} {split_amt:<10.2f} {str(init_qty):<10} {str(init_amt):<10} {str(end_qty):<10} {str(end_amt):<10}")

    # 再查一下是否有其他表有库存数据
    print("\n" + "="*100)
    print("\n查看是否有专门的库存表...")

    # 尝试查 inventory 相关表
    sql_tables = """
    SHOW TABLES FROM default_catalog.ads_business_analysis LIKE '%inventory%'
    """
    print("\n查找 inventory 相关表...")
    try:
        tables = query_sql(sql_tables)
        print(f"找到的表: {tables}")
    except Exception as e:
        print(f"查询失败: {e}")

    # 尝试查 stock 相关表
    sql_tables2 = """
    SHOW TABLES FROM default_catalog.ads_business_analysis LIKE '%stock%'
    """
    print("\n查找 stock 相关表...")
    try:
        tables2 = query_sql(sql_tables2)
        print(f"找到的表: {tables2}")
    except Exception as e:
        print(f"查询失败: {e}")

if __name__ == "__main__":
    main()