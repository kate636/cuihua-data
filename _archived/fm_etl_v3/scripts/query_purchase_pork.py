"""
查询 strategy_fm_purchase_di 表中猪肉类商品的数据（2026-04-23）
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

def main():
    # 查询猪肉类商品（category_level1_id = 13）的进货数据
    sql = """
    SELECT
        p.store_id,
        p.business_date,
        p.day_clear,
        p.article_id,
        p.article_name,
        p.sale_article_id,
        p.sale_article_name,
        p.sale_article_qty,
        p.sale_article_purchase_amt,
        p.init_stock_qty,
        p.init_stock_amt,
        p.end_stock_qty,
        p.end_stock_amt,
        p.avg_inbound_price,
        g.category_level1_id,
        g.category_level1_description,
        g.category_level2_id,
        g.category_level2_description
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di p
    LEFT JOIN default_catalog.ads_business_analysis.strategy_fm_dim_goods g
      ON g.article_id = p.sale_article_id
    WHERE p.inc_day = '2026-04-23'
      AND g.category_level1_id = '13'
    ORDER BY p.article_id, p.sale_article_id
    """

    print("查询猪肉类进货数据...")
    rows = query_sql(sql)

    if not rows:
        print("无数据")
        return

    print(f"\n共 {len(rows)} 条记录")
    print("\n" + "="*120)

    # 按 article_id 分组显示
    parents = {}
    for row in rows:
        aid = row.get('articleId', row.get('article_id', ''))
        if aid not in parents:
            parents[aid] = []
        parents[aid].append(row)

    for parent_id, subs in sorted(parents.items()):
        print(f"\n【验收条码 parent】={parent_id}")
        print("-"*120)

        for sub in subs:
            sale_id = sub.get('saleArticleId', sub.get('sale_article_id', ''))
            sale_name = sub.get('saleArticleName', sub.get('sale_article_name', ''))
            article_name = sub.get('articleName', sub.get('article_name', ''))
            day_clear = sub.get('dayClear', sub.get('day_clear', ''))

            # 数量金额
            qty = sub.get('saleArticleQty', sub.get('sale_article_qty', 0))
            amt = sub.get('saleArticlePurchaseAmt', sub.get('sale_article_purchase_amt', 0))
            avg_price = sub.get('avgInboundPrice', sub.get('avg_inbound_price', 0))

            # 库存
            init_qty = sub.get('initStockQty', sub.get('init_stock_qty', 0))
            init_amt = sub.get('initStockAmt', sub.get('init_stock_amt', 0))
            end_qty = sub.get('endStockQty', sub.get('end_stock_qty', 0))
            end_amt = sub.get('endStockAmt', sub.get('end_stock_amt', 0))

            # 分类
            cat1 = sub.get('categoryLevel1Description', sub.get('category_level1_description', ''))
            cat2 = sub.get('categoryLevel2Description', sub.get('category_level2_description', ''))

            is_bom = "✓ BOM" if parent_id != sale_id else "标品"

            print(f"  销售条码={sale_id} | {sale_name}")
            print(f"  验收名称={article_name}")
            print(f"  分类={cat1}/{cat2} | 日清={day_clear} | {is_bom}")
            print(f"  进货量={qty} | 进货额={amt} | 均价={avg_price}")
            print(f"  期初量={init_qty} | 期初额={init_amt}")
            print(f"  期末量={end_qty} | 期末额={end_amt}")

            # 计算单价
            if qty and qty > 0:
                unit_cost = amt / qty
                print(f"  单位成本(进货额/量)={unit_cost:.2f}")

            print()

    # 统计
    print("\n" + "="*120)
    print("统计汇总：")
    bom_count = sum(1 for r in rows if r.get('articleId', r.get('article_id', '')) != r.get('saleArticleId', r.get('sale_article_id', '')))
    std_count = len(rows) - bom_count
    print(f"  BOM 拆分商品: {bom_count} 条")
    print(f"  标品（验收=销售）: {std_count} 条")

    # 日清统计
    day_clear_0 = sum(1 for r in rows if r.get('dayClear', r.get('day_clear', '')) == '0')
    day_clear_1 = sum(1 for r in rows if r.get('dayClear', r.get('day_clear', '')) == '1')
    print(f"  日清(day_clear=0): {day_clear_0} 条")
    print(f"  非日清(day_clear=1): {day_clear_1} 条")

if __name__ == "__main__":
    main()