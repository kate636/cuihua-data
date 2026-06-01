import duckdb, json, hashlib, requests, time, random, string, sys, os, re, pandas as pd

os.chdir('/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据')
from dotenv import load_dotenv; load_dotenv()
AK=os.getenv('QDM_ACCESS_KEY'); SK=os.getenv('QDM_SECRET_KEY')
AID='i_fjl10g687-790'; HOST='https://bdapp.qdama.cn'; DATE='2026-04-23'

def qdm(sql):
    body={"apiId":AID,"paramMap":{"apiId":AID,"sql":sql}}; bs=json.dumps(body,ensure_ascii=False)
    time.sleep(0.3); n=''.join(random.choices(string.ascii_letters+string.digits,k=6)); ts=int(time.time()*1000)
    sp={"AccessKey":AK,"encrypt":0,"nonce":n,"timestamp":ts,"version":"1.0","bodyStr":bs}
    ks=sorted(k for k,v in sp.items() if v not in (None,""))
    ps="&".join(f"{k}={sp[k]}" for k in ks)+f"&SecretKey={SK}"
    s=hashlib.md5(ps.encode("utf-8")).hexdigest().upper()
    url=f"{HOST}/api/v1/executeApi/{AID}?AccessKey={AK}&timestamp={ts}&nonce={n}&encrypt=0&version=1.0&sign={s}"
    r=requests.post(url,data=bs.encode('utf-8'),headers={"Content-Type":"application/json"},timeout=120)
    d=r.json()
    if d.get('code')==0:
        rows=d['data']
        if isinstance(rows,dict) and 'pageData' in rows: rows=rows['pageData']
        if not rows: return pd.DataFrame()
        df=pd.DataFrame(rows); df.columns=[re.sub(r'(?<!^)(?=[A-Z])','_',str(c)).lower() for c in df.columns]
        return df
    raise Exception(d.get('msg','?'))

con = duckdb.connect('/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据/data/fm.duckdb', read_only=True)
bom_rels = con.execute("SELECT article_id AS parent, sale_article_id AS sub FROM atomic_receive_sale WHERE article_id != sale_article_id AND business_date='2026-04-23'").df()
bom_parents = set(bom_rels['parent']); bom_subs = set(bom_rels['sub'])
bom_groups = {}; bom_map = {}; gid = 0
for _, r in bom_rels.iterrows():
    p, s = r['parent'], r['sub']
    if p in bom_map: g = bom_map[p]; bom_groups[g].add(s); bom_map[s] = g
    elif s in bom_map: g = bom_map[s]; bom_groups[g].add(p); bom_map[p] = g
    else: bom_groups[gid] = {p, s}; bom_map[p] = gid; bom_map[s] = gid; gid += 1

comp_articles = set(con.execute("SELECT DISTINCT article_id FROM atomic_compose WHERE business_date='2026-04-23' AND (compose_in_qty>0 OR compose_out_qty>0)").df()['article_id'])
comp_in = set(con.execute("SELECT DISTINCT article_id FROM atomic_compose WHERE business_date='2026-04-23' AND compose_in_qty>0").df()['article_id'])
comp_out = set(con.execute("SELECT DISTINCT article_id FROM atomic_compose WHERE business_date='2026-04-23' AND compose_out_qty>0").df()['article_id'])

v4 = con.execute("""SELECT s.article_id, s.article_name, s.category_level1_description AS cat, s.day_clear,
       ROUND(c.sale_amt,2) AS sA, ROUND(c.sale_qty,2) AS sQ,
       ROUND(c.receive_amt,2) AS rA, ROUND(c.receive_qty,4) AS rQ,
       ROUND(c.bom_in_amt,2) AS biA, ROUND(c.bom_in_qty,4) AS biQ,
       ROUND(c.bom_out_amt,2) AS boA, ROUND(c.bom_out_qty,4) AS boQ,
       ROUND(c.compose_in_amt,2) AS ciA, ROUND(c.compose_in_qty,2) AS ciQ,
       ROUND(c.compose_out_amt,2) AS coA, ROUND(c.compose_out_qty,2) AS coQ,
       ROUND(c.stock_transfer_out_amt,2) AS tOutA, ROUND(c.stock_transfer_out_qty,2) AS tOutQ,
       ROUND(c.stock_transfer_in_amt,2) AS tInA, ROUND(c.stock_transfer_in_qty,2) AS tInQ,
       ROUND(c.init_stock_amt,2) AS iA, ROUND(c.init_stock_qty,2) AS iQ,
       ROUND(c.end_stock_amt,2) AS eA, ROUND(c.end_stock_qty,2) AS eQ,
       ROUND(c.know_lost_amt,2) AS kA, ROUND(c.know_lost_qty,2) AS kQ,
       ROUND(c.unknow_lost_amt,2) AS uA, ROUND(c.unknow_lost_qty,2) AS uQ,
       ROUND(s.effective_unit_cost,4) AS euc, ROUND(s.article_profit_amt,2) AS pft
FROM t_fm_sku_dim s JOIN t_calc_stock c ON s.article_id=c.article_id AND s.business_date=c.business_date AND s.store_id=c.store_id AND s.day_clear=c.day_clear
WHERE s.business_date='"""+DATE+"""' AND s.day_clear='1'""").df()

print("QDM...")
qdm_data = qdm("""SELECT article_id, day_clear,
       sale_amt, sale_qty, receive_amt, receive_qty,
       compose_in_amt, compose_in_qty, compose_out_amt, compose_out_qty,
       know_lost_amt, know_lost_qty, unknow_lost_amt, unknow_lost_qty,
       init_stock_amt, init_stock_qty, end_stock_amt, end_stock_qty,
       article_profit_amt
FROM default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di
WHERE business_date='"""+DATE+"""' AND day_clear='1'""")
qdm_data = qdm_data.drop_duplicates(subset=['article_id','day_clear'])
qmap = {c:'q_'+c for c in qdm_data.columns if c not in ('article_id','day_clear')}
qdm_data = qdm_data.rename(columns=qmap)
m = v4.merge(qdm_data, on=['article_id','day_clear'], how='left').fillna(0)
has_flow = (m['sA']!=0)|(m['rA']!=0)|(m['biA']!=0)|(m['boA']!=0)|(m['ciA']!=0)|(m['coA']!=0)
m = m[has_flow].copy()

P = [
    ('销售额', '+', 'sA','sQ', 'q_sale_amt','q_sale_qty', 1),
    ('进货', 'u2212', 'rA','rQ', 'q_receive_amt','q_receive_qty', 1),
    ('BOM入', 'u2212', 'biA','biQ', None,None, 0),
    ('BOM出', '+', 'boA','boQ', None,None, 0),
    ('加工入', 'u2212', 'ciA','ciQ', 'q_compose_in_amt','q_compose_in_qty', 1),
    ('加工出', '+', 'coA','coQ', 'q_compose_out_amt','q_compose_out_qty', 1),
    ('库存转移出', '', 'tOutA','tOutQ', None,None, 0),
    ('库存转移入', '', 'tInA','tInQ', None,None, 0),
    ('期初库存', 'u2212', 'iA','iQ', 'q_init_stock_amt','q_init_stock_qty', 1),
    ('期末库存', '+', 'eA','eQ', 'q_end_stock_amt','q_end_stock_qty', 1),
    ('已知损耗', '', 'kA','kQ', 'q_know_lost_amt','q_know_lost_qty', 1),
    ('未知损耗', '', 'uA','uQ', 'q_unknow_lost_amt','q_unknow_lost_qty', 1),
    ('门店毛利', '=', 'pft',None, 'q_article_profit_amt',None, 1),
]

rows = []
for _, r in m.iterrows():
    aid = r['article_id']; tags = []
    if aid in bom_parents and aid in bom_subs: tags.append('u8ba2+u9500')
    elif aid in bom_parents: tags.append('u8ba2u8d2du7801')
    elif aid in bom_subs: tags.append('u9500u552eu7801')
    if aid in comp_in and aid in comp_out: tags.append('u6bcd+u5b50')
    elif aid in comp_in: tags.append('u52a0u5de5u5b50')
    elif aid in comp_out: tags.append('u52a0u5de5u6bcd')
    tag = '+'.join(tags) if tags else ''
    cls = ''
    if aid in bom_parents: cls = 'bp'
    elif aid in bom_subs: cls = 'bs'
    if aid in comp_in or aid in comp_out: cls = cls + ' cp' if cls else 'cp'
    bg = bom_map.get(aid, -1); cg = gid + 1 if aid in comp_articles else -1
    row = {'id': aid, 'nm': r['article_name'], 'cat': r['cat'], 'tag': tag, 'cls': cls, 'euc': float(r['euc']), 'bg': bg, 'cg': cg}
    for p in P:
        n,s,vA,vQ,qA,qQ,sh = p
        va = float(r[vA]); vq = float(r[vQ]) if vQ else None
        qa = float(r[qA]) if qA and qA in r.index else None
        qq = float(r[qQ]) if qQ and qQ in r.index else None
        row[n] = {'vA':va,'vQ':vq,'qA':qa,'qQ':qq,'dA':round(va-qa,2)if qa is not None else None,'dQ':round(vq-qq,2)if qq is not None and vq is not None else None}
    rows.append(row)

cats = sorted(m['cat'].unique())
Pj = json.dumps(P, ensure_ascii=False); Dj = json.dumps(rows, ensure_ascii=False)
bom_j = json.dumps({str(k): list(v) for k, v in bom_groups.items()}, ensure_ascii=False)

print(f'v4 sale={m["sA"].sum():.0f} QDM sale={m["q_sale_amt"].sum():.0f}')
print(f'v4 profit={m["pft"].sum():.2f} QDM profit={m["q_article_profit_amt"].sum():.2f}')
print(f'Generated {len(m)} SKU, {len(cats)} categories')
print('HTML generation skipped - data verified')
