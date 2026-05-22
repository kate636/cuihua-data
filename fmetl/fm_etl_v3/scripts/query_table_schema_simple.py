"""
查询 strategy_fm_* 表的完整字段信息（简化版，不依赖pandas）
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

# API 配置
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
    """执行 SQL 查询"""
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

# 要查询的表列表
TABLES = [
    ("strategy_fm_sales_di", 1),
    ("strategy_fm_purchase_di", 2),
    ("strategy_fm_scm_di", 3),
    ("strategy_fm_scm_adjust_di", 4),
    ("strategy_fm_loss_di", 5),
    ("strategy_fm_compose_di", 6),
    ("strategy_fm_allowance_di", 7),
    ("strategy_fm_promo_di", 8),
    ("strategy_fm_inventory_pool_di", 9),
    ("strategy_fm_price_da", 10),
    ("strategy_fm_dim_day_clear", 12),
    ("strategy_fm_dim_store_profile", 13),
    ("strategy_fm_dim_saleable", 14),
    ("strategy_fm_dim_goods", 15),
    ("strategy_fm_dim_calendar", 16),
    ("strategy_fm_receive_sale_di", 17),
    ("strategy_fm_order_receive_di", 18),
    ("strategy_fm_dim_article_convert", 19),
    ("strategy_dim_store_article_bom_relation", 20),
]

def main():
    for table_name, seq in TABLES:
        print(f"\n{'='*80}")
        print(f"[{seq}] {table_name}")
        print(f"{'='*80}")

        # DESC 查询
        sql = f"DESC default_catalog.ads_business_analysis.{table_name}"
        try:
            rows = query_sql(sql)
            if rows:
                print(f"\n字段数: {len(rows)}")
                print("\n字段详情:")
                print("-" * 80)
                for row in rows:
                    field = row.get('Field', row.get('field', ''))
                    type_ = row.get('Type', row.get('type', ''))
                    null = row.get('Null', row.get('null', ''))
                    key = row.get('Key', row.get('key', ''))
                    default = row.get('Default', row.get('default', ''))
                    extra = row.get('Extra', row.get('extra', ''))
                    comment = row.get('Comment', row.get('comment', ''))
                    print(f"  {field:<35} {type_:<20} {comment}")
        except Exception as e:
            print(f"DESC 查询失败: {e}")

        # 样本数据查询（LIMIT 1）
        sql2 = f"SELECT * FROM default_catalog.ads_business_analysis.{table_name} WHERE inc_day = '2026-04-23' LIMIT 1"
        try:
            rows2 = query_sql(sql2)
            if rows2:
                print(f"\n样本数据列: {len(rows2[0]) if rows2 else 0}")
                print("\n样本数据字段:")
                for k in sorted(rows2[0].keys()):
                    v = rows2[0][k]
                    v_str = str(v)[:50] if v is not None else 'NULL'
                    print(f"  {k:<35} = {v_str}")
        except Exception as e:
            print(f"样本数据查询失败: {e}")

if __name__ == "__main__":
    main()