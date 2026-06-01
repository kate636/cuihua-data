"""
从 inventory_pool_di 表查看排骨的期初库存数据
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
    print("查看 inventory_pool_di 表的库存数据（2026-04-23）")
    print("="*100)

    # 先看表结构
    sql_schema = """
    DESCRIBE default_catalog.ads_business_analysis.strategy_fm_inventory_pool_di
    """
    print("\n1. 表结构...")
    try:
        schema = query_sql(sql_schema)
        print(f"共 {len(schema)} 个字段：")
        for f in schema:
            name = f.get('col_name', f.get('columnName', f.get('name', '')))
            type_ = f.get('data_type', f.get('dataType', f.get('type', '')))
            print(f"  {name}: {type_}")
    except Exception as e:
        print(f"查询失败: {e}")

    # 查排骨的库存数据
    print("\n" + "="*100)
    print("\n2. 查排骨(20003470)的库存数据...")

    sql = """
    SELECT *
    FROM default_catalog.ads_business_analysis.strategy_fm_inventory_pool_di
    WHERE inc_day = '2026-04-23'
      AND shop_id = 'A3XV'
      AND sku_code = '20003470'
    LIMIT 10
    """
    rows = query_sql(sql)
    print(f"共 {len(rows)} 条记录")

    if rows:
        print("\n原始数据：")
        print(json.dumps(rows[0], indent=2, ensure_ascii=False))

        # 尝试找库存量和成本价字段
        for r in rows:
            print(f"\n字段列表: {list(r.keys())}")
            break
    else:
        print("无数据")

    # 尝试不同字段名
    print("\n" + "="*100)
    print("\n3. 尝试查看当天所有库存数据（前10条）...")

    sql_all = """
    SELECT *
    FROM default_catalog.ads_business_analysis.strategy_fm_inventory_pool_di
    WHERE inc_day = '2026-04-23'
      AND shop_id = 'A3XV'
    LIMIT 10
    """
    rows_all = query_sql(sql_all)
    print(f"共 {len(rows_all)} 条记录")

    if rows_all:
        print("\n第一条数据：")
        print(json.dumps(rows_all[0], indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()