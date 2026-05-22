"""
查看 parent 20500351 和 21153037 各有哪些 sub SKU
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
        raise RuntimeError(f"API error: {result.get('code')}, {result.get('msg')}")
    data = result.get("data")
    if isinstance(data, dict) and "pageData" in data:
        return data["pageData"]
    if isinstance(data, list):
        return data
    return []

def safe_float(val, default=0.0):
    if val is None: return default
    try: return float(val)
    except: return default

def main():
    print("="*100)
    print("Parent 20500351 和 21153037 的 sub SKU 列表（2026-04-23）")
    print("="*100)

    # Parent 1: 20500351
    sql1 = """
    SELECT
        sale_article_id AS sub_id,
        sale_article_name AS sub_name,
        sale_article_qty AS split_qty,
        sale_article_purchase_amt AS split_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND article_id = '20500351'
    ORDER BY sale_article_id
    """
    print("\n【Parent 20500351 - 优鲜大白猪A级】")
    rows1 = query_sql(sql1)
    print(f"共 {len(rows1)} 个 sub SKU：")
    print(f"\n{'SubID':<12} {'名称':<25} {'拆分量(kg)':<12} {'拆分额(元)':<12}")
    print("-"*70)

    # 标记排骨
    paigu_id = '20003470'
    for r in rows1:
        sub_id = r.get('subId', r.get('sub_id', ''))
        sub_name = str(r.get('subName', r.get('sub_name', '')))[:25]
        qty = safe_float(r.get('splitQty', r.get('split_qty')))
        amt = safe_float(r.get('splitAmt', r.get('split_amt')))
        mark = " ← 排骨" if sub_id == paigu_id else ""
        print(f"{sub_id:<12} {sub_name:<25} {qty:<12.4f} {amt:<12.2f}{mark}")

    # Parent 2: 21153037
    sql2 = """
    SELECT
        sale_article_id AS sub_id,
        sale_article_name AS sub_name,
        sale_article_qty AS split_qty,
        sale_article_purchase_amt AS split_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND article_id = '21153037'
    ORDER BY sale_article_id
    """
    print("\n" + "="*100)
    print("\n【Parent 21153037 - 优鲜带颈通排】")
    rows2 = query_sql(sql2)
    print(f"共 {len(rows2)} 个 sub SKU：")
    print(f"\n{'SubID':<12} {'名称':<25} {'拆分量(kg)':<12} {'拆分额(元)':<12}")
    print("-"*70)

    for r in rows2:
        sub_id = r.get('subId', r.get('sub_id', ''))
        sub_name = str(r.get('subName', r.get('sub_name', '')))[:25]
        qty = safe_float(r.get('splitQty', r.get('split_qty')))
        amt = safe_float(r.get('splitAmt', r.get('split_amt')))
        mark = " ← 排骨" if sub_id == paigu_id else ""
        print(f"{sub_id:<12} {sub_name:<25} {qty:<12.4f} {amt:<12.2f}{mark}")

    # 检查是否有重叠的sub
    subs1 = set(r.get('subId', r.get('sub_id')) for r in rows1)
    subs2 = set(r.get('subId', r.get('sub_id')) for r in rows2)
    overlap = subs1 & subs2

    print("\n" + "="*100)
    print("\n【重叠分析】")
    print(f"Parent1 的 sub 数量: {len(subs1)}")
    print(f"Parent2 的 sub 数量: {len(subs2)}")
    print(f"重叠的 sub 数量: {len(overlap)}")

    if overlap:
        print(f"\n重叠的 sub SKU：")
        for s in overlap:
            # 找到名称
            name1 = next((r.get('subName', r.get('sub_name', '')) for r in rows1 if r.get('subId', r.get('sub_id')) == s), '')
            name2 = next((r.get('subName', r.get('sub_name', '')) for r in rows2 if r.get('subId', r.get('sub_id')) == s), '')
            qty1 = next((safe_float(r.get('splitQty', r.get('split_qty'))) for r in rows1 if r.get('subId', r.get('sub_id')) == s), 0)
            qty2 = next((safe_float(r.get('splitQty', r.get('split_qty'))) for r in rows2 if r.get('subId', r.get('sub_id')) == s), 0)
            print(f"  {s} {name1}")
            print(f"    从 parent1 拆分: {qty1:.4f} kg")
            print(f"    从 parent2 拆分: {qty2:.4f} kg")
            print(f"    合计: {qty1 + qty2:.4f} kg")

if __name__ == "__main__":
    main()