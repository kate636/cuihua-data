import duckdb, json, hashlib, requests, time, random, string, os, re, pandas as pd

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

v4 = con.execute(f"""
SELECT s.article_id, s.article_name, s.category_level1_description AS cat, s.day_clear,
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
WHERE s.business_date='{DATE}' AND s.day_clear IN ('0','1')
""").df()

print("QDM...")
qdm_data = qdm(f"""
SELECT article_id, day_clear,
       SUM(sale_amt) AS sale_amt, SUM(sale_qty) AS sale_qty,
       SUM(receive_amt) AS receive_amt, SUM(receive_qty) AS receive_qty,
       SUM(compose_in_amt) AS compose_in_amt, SUM(compose_in_qty) AS compose_in_qty,
       SUM(compose_out_amt) AS compose_out_amt, SUM(compose_out_qty) AS compose_out_qty,
       SUM(know_lost_amt) AS know_lost_amt, SUM(know_lost_qty) AS know_lost_qty,
       SUM(unknow_lost_amt) AS unknow_lost_amt, SUM(unknow_lost_qty) AS unknow_lost_qty,
       SUM(init_stock_amt) AS init_stock_amt, SUM(init_stock_qty) AS init_stock_qty,
       SUM(end_stock_amt) AS end_stock_amt, SUM(end_stock_qty) AS end_stock_qty,
       SUM(article_profit_amt) AS article_profit_amt
FROM (
    SELECT store_id, article_id, day_clear,
           MAX(sale_amt) AS sale_amt, MAX(sale_qty) AS sale_qty,
           MAX(receive_amt) AS receive_amt, MAX(receive_qty) AS receive_qty,
           MAX(compose_in_amt) AS compose_in_amt, MAX(compose_in_qty) AS compose_in_qty,
           MAX(compose_out_amt) AS compose_out_amt, MAX(compose_out_qty) AS compose_out_qty,
           MAX(know_lost_amt) AS know_lost_amt, MAX(know_lost_qty) AS know_lost_qty,
           MAX(unknow_lost_amt) AS unknow_lost_amt, MAX(unknow_lost_qty) AS unknow_lost_qty,
           MAX(init_stock_amt) AS init_stock_amt, MAX(init_stock_qty) AS init_stock_qty,
           MAX(end_stock_amt) AS end_stock_amt, MAX(end_stock_qty) AS end_stock_qty,
           MAX(article_profit_amt) AS article_profit_amt
    FROM default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di
    WHERE business_date='{DATE}' AND day_clear IN ('0','1')
    GROUP BY store_id, article_id, day_clear
) t
GROUP BY article_id, day_clear
""")
qdm_data = qdm_data.drop_duplicates(subset=['article_id'])
qmap = {c:'q_'+c for c in qdm_data.columns if c not in ('article_id','day_clear')}
qdm_data = qdm_data.rename(columns=qmap)
m = v4.merge(qdm_data.drop(columns=['day_clear'], errors='ignore'), on='article_id', how='outer')
for c in ['cat','article_name']:
    if c in m.columns: m[c] = m[c].fillna('QDM-only')
m = m.fillna(0)
# Keep all rows - every row in t_fm_sku_dim has meaningful data

P = [
    ('销售额', '+', 'sA','sQ', 'q_sale_amt','q_sale_qty', 1),
    ('进货', '−', 'rA','rQ', 'q_receive_amt','q_receive_qty', 1),
    ('BOM入', '−', 'biA','biQ', None,None, 0),
    ('BOM出', '+', 'boA','boQ', None,None, 0),
    ('加工入', '−', 'ciA','ciQ', 'q_compose_in_amt','q_compose_in_qty', 1),
    ('加工出', '+', 'coA','coQ', 'q_compose_out_amt','q_compose_out_qty', 1),
    ('库存转出', '', 'tOutA','tOutQ', None,None, 0),
    ('库存转入', '', 'tInA','tInQ', None,None, 0),
    ('期初库存', '−', 'iA','iQ', 'q_init_stock_amt','q_init_stock_qty', 1),
    ('期末库存', '+', 'eA','eQ', 'q_end_stock_amt','q_end_stock_qty', 1),
    ('已知损耗', '', 'kA','kQ', 'q_know_lost_amt','q_know_lost_qty', 1),
    ('未知损耗', '', 'uA','uQ', 'q_unknow_lost_amt','q_unknow_lost_qty', 1),
    ('门店毛利', '=', 'pft',None, 'q_article_profit_amt',None, 1),
]

rows = []
for _, r in m.iterrows():
    aid = r['article_id']; tags = []
    if aid in bom_parents and aid in bom_subs: tags.append('订+销')
    elif aid in bom_parents: tags.append('订购码')
    elif aid in bom_subs: tags.append('销售码')
    if aid in comp_in and aid in comp_out: tags.append('母+子')
    elif aid in comp_in: tags.append('加工子')
    elif aid in comp_out: tags.append('加工母')
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

S = '−'  # minus sign
hdr1=''; hdr2=''
for p in P:
    n,s,_,_,_,_,sh = p
    colspan = 6 if sh else 2; cls = 'fc' if s in ('+',S,'=') else 'rc'
    hdr1 += f'<th class="{cls}" colspan="{colspan}">{s} {n}</th>'
    hdr2 += '<th>v4额</th><th>Q额</th><th>Δ额</th><th>v4量</th><th>Q量</th><th>Δ量</th>' if sh else '<th>v4额</th><th>v4量</th>'

html = f'''<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>v4 vs QDM {DATE}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,"Microsoft YaHei",sans-serif;font-size:12px;background:#f5f5f5;color:#333}}
.hdr{{background:linear-gradient(135deg,#1a1a2e,#16213e);color:#fff;padding:8px 14px}}
.hdr h1{{font-size:14px}} .hdr s{{font-size:9px;opacity:.4;margin-left:8px}}
.fm{{margin:6px 14px;padding:6px 12px;background:#fffbe6;border:1px solid #ffe58f;border-radius:5px;font-size:10px;text-align:center;font-family:"SF Mono",Menlo,monospace}}
.fm .p{{color:#cf1322;font-weight:700}} .fm .m{{color:#1890ff;font-weight:700}} .fm .e{{color:#389e0d;font-weight:700}} .fm .x{{color:#999}}
.leg{{margin:4px 14px;display:flex;gap:10px;font-size:9px}}
.leg span{{padding:1px 6px;border-radius:2px;border:1px solid #ddd}}
.leg .lbp{{background:#e3f2fd;border-color:#90caf9}} .leg .lbs{{background:#fff3e0;border-color:#ffcc80}} .leg .lcp{{background:#e8f5e9;border-color:#a5d6a7}}
.bar{{background:#fff;margin:0 14px;padding:5px 10px;display:flex;gap:6px;align-items:center;border:1px solid #e8e8e8;border-radius:5px 5px 0 0;flex-wrap:wrap;font-size:10px}}
.bar select,.bar input{{padding:3px 5px;border:1px solid #d9d9d9;border-radius:3px;font-size:10px}}
.bar label{{color:#888}}#info{{margin-left:auto;color:#888;font-size:10px}}
.tbl-outer{{margin:0 14px 14px;border:1px solid #e8e8e8;border-top:none;border-radius:0 0 5px 5px;overflow-x:auto;background:#fff}}
.tbl-inner{{min-width:2600px}}
th{{background:#1a1a2e;color:#fff;padding:2px 3px;text-align:right;white-space:nowrap;font-weight:500;font-size:7px}}
th.stk{{text-align:left;position:sticky;z-index:3;background:#16213e}}
th.c1{{left:0;min-width:82px;max-width:82px;font-size:10px}}
th.c2{{left:82px;min-width:88px;max-width:88px;font-size:10px}}
th.c3{{left:170px;min-width:58px;max-width:58px;font-size:10px}}
th.tc{{text-align:center;position:sticky;left:228px;z-index:3;background:#333;font-size:7px;min-width:42px;max-width:42px}}
th.fc{{background:#0f3460;text-align:center;font-size:7px;font-weight:400}}
th.rc{{background:#444;text-align:center;font-size:7px;font-weight:400}}
td{{padding:2px 3px;text-align:right;border-bottom:1px solid #f5f5f5;font-size:9px;white-space:nowrap}}
td.stk{{text-align:left;font-weight:500;position:sticky;background:#fff;z-index:1;font-size:10px}}
td.c1{{left:0;min-width:82px;max-width:82px}}td.c2{{left:82px;min-width:88px;max-width:88px}}td.c3{{left:170px;min-width:58px;max-width:58px;font-size:9px}}
td.tc{{text-align:center;font-size:7px;font-weight:600;position:sticky;left:228px;background:#fff;z-index:1;min-width:42px;max-width:42px}}
tr.bp td.stk,tr.bp td.tc{{background:#e3f2fd!important}}
tr.bs td.stk,tr.bs td.tc{{background:#fff3e0!important}}
tr.cp td.stk,tr.cp td.tc{{background:#e8f5e9!important}}
tr.bp.cp td.stk,tr.bp.cp td.tc{{background:#e0f2f1!important}}
tr.bs.cp td.stk,tr.bs.cp td.tc{{background:#f1f8e9!important}}
tr.hl-bom td{{background:#bbdefb!important;outline:2px solid #1565c0;outline-offset:-1px;z-index:5}}
tr.hl-bom td.stk,tr.hl-bom td.tc{{background:#bbdefb!important}}
tr.hl-comp td{{background:#c8e6c9!important;outline:2px solid #2e7d32;outline-offset:-1px;z-index:5}}
tr.hl-comp td.stk,tr.hl-comp td.tc{{background:#c8e6c9!important}}
tr:hover td{{background:#e6f7ff!important}}tr:hover td.stk,tr:hover td.tc{{background:#e6f7ff!important}}
tr.tot td{{background:#fafafa!important;border-top:2px solid #1a1a2e;border-bottom:2px solid #1a1a2e;font-weight:700;font-size:9px}}
tr.tot td.stk,tr.tot td.tc{{background:#fafafa!important}}
.df{{font-weight:700}}.df.p{{color:#cf1322}}.df.n{{color:#389e0d}}.df.o{{color:#bbb}}
.q{{color:#999}}
.cards{{display:flex;gap:6px;flex-wrap:wrap;margin:0 14px 4px}}
.cd{{background:#fff;padding:4px 8px;border-radius:4px;border:1px solid #e8e8e8;text-align:center;min-width:55px}}
.cd .vl{{font-size:12px;font-weight:700}}.cd .lb{{font-size:7px;color:#999}}
</style></head><body>
<div class="hdr"><h1>SKU全字段对比 v4 vs QDM<s>{DATE}</s></h1></div>
<div class="fm"><span class="e">门店毛利</span> = <span class="p">+销售额</span> <span class="m">{S}进货</span> <span class="m">{S}BOM入</span> <span class="p">+BOM出</span> <span class="m">{S}加工入</span> <span class="p">+加工出</span> <span class="p">+期末库存</span> <span class="m">{S}期初库存</span> <span class="x">[转出/入=参考]</span></div>
<div class="leg"><span class="lbp">订购码</span><span class="lbs">销售码</span><span class="lcp">加工</span> &nbsp; <b>点击筛选</b>: 点击SKU仅显示关联SKU | 再点还原</div>
<div class="bar">
<label>分类</label><select id="cf" onchange="f()"><option value="">全部{len(cats)}分类</option>{''.join(f'<option value="{c}">{c}</option>' for c in cats)}</select>
<label>搜索</label><input id="kw" placeholder="编码/名称..." oninput="f()" style="width:110px">
<label>标记</label><select id="tf" onchange="f()"><option value="">全部</option><option value="bp">订购码</option><option value="bs">销售码</option><option value="cp">加工</option></select>
<label>毛利差≥</label><select id="th" onchange="f()"><option value="0">全部</option><option value="5">5</option><option value="10">10</option><option value="50">50</option></select>
<label>排序</label><select id="so" onchange="f()"><option value="d">毛利差↓</option><option value="u">未知损耗差↓</option><option value="p">毛利↓</option><option value="c">分类</option></select>
<span id="info"></span>
</div>
<div class="cards" id="cards"></div>
<div class="tbl-outer"><div class="tbl-inner"><table><thead>
<tr><th class="stk c1">编码</th><th class="stk c2">名称</th><th class="stk c3">分类</th><th class="tc">标记</th>{hdr1}<th class="fc">euc</th></tr>
<tr><th class="stk c1"></th><th class="stk c2"></th><th class="stk c3"></th><th class="tc"></th>{hdr2}<th>v4</th></tr>
</thead><tbody id="body"></tbody></table></div></div>
<script>
const D={Dj},P={Pj},BOM={bom_j};
let idx={{}}; D.forEach((r,i)=>{{idx[r.id]=i;}});
function rr(r){{
  let h=`<tr class="${{r.cls||''}}" id="r_${{r.id}}" data-row="${{r.id}}" onclick="clickFilter('${{r.id}}')" style="cursor:pointer">`;
  h+=`<td class="stk c1">${{r.id}}</td><td class="stk c2">${{r.nm||''}}</td><td class="stk c3" style="font-size:9px">${{r.cat}}</td><td class="tc">${{r.tag||''}}</td>`;
  P.forEach(p=>{{
    let n=p[0],sh=p[6],d=r[n];if(!d){{h+=sh?'<td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>':'<td>-</td><td>-</td>';return;}}
    let da=d.dA,dc=da==null?'o':(Math.abs(da)<.001?'o':(da>0?'p':'n'));
    let dq=d.dQ,dcq=dq==null?'o':(Math.abs(dq||0)<.001?'o':((dq||0)>0?'p':'n'));
    if(sh) h+=`<td>${{d.vA.toFixed(2)}}</td><td class="q">${{d.qA!=null?d.qA.toFixed(2):'-'}}</td><td class="df ${{dc}}">${{da!=null?da.toFixed(2):'-'}}</td><td>${{d.vQ!=null?d.vQ.toFixed(2):'-'}}</td><td class="q">${{d.qQ!=null?d.qQ.toFixed(2):'-'}}</td><td class="df ${{dcq}}">${{dq!=null?dq.toFixed(2):'-'}}</td>`;
    else h+=`<td>${{d.vA.toFixed(2)}}</td><td>${{d.vQ!=null?d.vQ.toFixed(2):'-'}}</td>`;
  }});h+=`<td>${{r.euc.toFixed(4)}}</td></tr>`;return h;
}}
let filteredIds=null;
function clickFilter(aid){{
  if(filteredIds){{filteredIds=null;document.querySelectorAll('tr[data-row]').forEach(el=>el.style.display='');rebuildTotals();document.getElementById('info').innerHTML=D.length+' SKU <span style="color:#1565c0">[点击筛选关联SKU]</span>';return;}}
  let d=D[idx[aid]];if(!d)return;let ids=new Set([aid]);
  if(d.bg>=0&&BOM[d.bg])BOM[d.bg].forEach(id=>ids.add(id));
  if(d.cg>=0)D.forEach(r=>{{if(r.cg>=0)ids.add(r.id);}});
  filteredIds=ids;document.querySelectorAll('tr[data-row]').forEach(el=>{{el.style.display=ids.has(el.getAttribute('data-row'))?'':'none';}});
  rebuildTotals();document.getElementById('info').innerHTML=ids.size+' SKU <span style="color:#c62828">[点击还原全部]</span>';
}}
function rebuildTotals(){{
  let tot={{}};P.forEach(p=>{{let n=p[0];tot[n]={{s4A:0,s4Q:0,sQA:0,sQQ:0}};}});
  D.forEach(r=>{{if(filteredIds&&!filteredIds.has(r.id))return;P.forEach(p=>{{let n=p[0],d=r[n];if(d){{tot[n].s4A+=d.vA||0;tot[n].s4Q+=d.vQ||0;tot[n].sQA+=d.qA||0;tot[n].sQQ+=d.qQ||0;}}}});}});
  let tr=document.querySelector('tr.tot');if(!tr)return;let tds=tr.querySelectorAll('td');
  tds[0].textContent='合计';tds[1].textContent=(filteredIds?filteredIds.size:D.length)+'SKU';tds[2].textContent='';tds[3].textContent='';let ci=4;
  P.forEach(p=>{{let n=p[0],sh=p[6],t=tot[n],dA=t.s4A-t.sQA,dQ=t.s4Q-t.sQQ,ca=Math.abs(dA)<.001?'o':(dA>0?'p':'n'),cq=Math.abs(dQ)<.001?'o':(dQ>0?'p':'n');
    if(sh){{tds[ci].textContent=t.s4A.toFixed(2);tds[ci+1].textContent=t.sQA.toFixed(2);tds[ci+2].textContent=dA.toFixed(2);tds[ci+2].className='df '+ca;tds[ci+3].textContent=t.s4Q.toFixed(2);tds[ci+4].textContent=t.sQQ.toFixed(2);tds[ci+5].textContent=dQ.toFixed(2);tds[ci+5].className='df '+cq;ci+=6;}}
    else{{tds[ci].textContent=t.s4A.toFixed(2);tds[ci+1].textContent=t.s4Q.toFixed(2);ci+=2;}}
  }});tds[ci].textContent='';
}}
function f(){{
  let cf=document.getElementById('cf').value,tf=document.getElementById('tf').value,kw=document.getElementById('kw').value.toLowerCase(),th=parseFloat(document.getElementById('th').value),so=document.getElementById('so').value;
  let fd=[...D];
  if(cf) fd=fd.filter(r=>r.cat===cf);if(tf) fd=fd.filter(r=>r.cls.includes(tf));
  if(kw) fd=fd.filter(r=>r.id.includes(kw)||(r.nm||'').toLowerCase().includes(kw));
  if(th>0) fd=fd.filter(r=>r['门店毛利']&&Math.abs(r['门店毛利'].dA)>th);
  if(so==='u') fd.sort((a,b)=>Math.abs(b['未知损耗']?.dA||0)-Math.abs(a['未知损耗']?.dA||0));
  else if(so==='d') fd.sort((a,b)=>Math.abs(b['门店毛利']?.dA||0)-Math.abs(a['门店毛利']?.dA||0));
  else if(so==='p') fd.sort((a,b)=>b['门店毛利'].vA-a['门店毛利'].vA);
  else fd.sort((a,b)=>a.cat.localeCompare(b.cat)||b['门店毛利'].vA-a['门店毛利'].vA);
  let tot={{}};P.forEach(p=>{{let n=p[0];tot[n]={{s4A:0,s4Q:0,sQA:0,sQQ:0}};}});
  fd.forEach(r=>{{P.forEach(p=>{{let n=p[0],d=r[n];if(d){{tot[n].s4A+=d.vA||0;tot[n].s4Q+=d.vQ||0;tot[n].sQA+=d.qA||0;tot[n].sQQ+=d.qQ||0;}}}});}});
  let tr='<tr class="tot"><td class="stk c1">合计</td><td class="stk c2">'+fd.length+'SKU</td><td class="stk c3"></td><td class="tc"></td>';
  P.forEach(p=>{{let n=p[0],sh=p[6],t=tot[n],dA=t.s4A-t.sQA,dQ=t.s4Q-t.sQQ,ca=Math.abs(dA)<.001?'o':(dA>0?'p':'n'),cq=Math.abs(dQ)<.001?'o':(dQ>0?'p':'n');
    if(sh) tr+=`<td>${{t.s4A.toFixed(2)}}</td><td class="q">${{t.sQA.toFixed(2)}}</td><td class="df ${{ca}}">${{dA.toFixed(2)}}</td><td>${{t.s4Q.toFixed(2)}}</td><td class="q">${{t.sQQ.toFixed(2)}}</td><td class="df ${{cq}}">${{dQ.toFixed(2)}}</td>`;
    else tr+=`<td>${{t.s4A.toFixed(2)}}</td><td>${{t.s4Q.toFixed(2)}}</td>`;
  }});tr+='<td></td></tr>';
  document.getElementById('body').innerHTML=tr+fd.map(rr).join('');
  document.getElementById('info').textContent=`${{fd.length}} SKU`;
  let t=tot;let tp=t['门店毛利'].s4A-t['门店毛利'].sQA,tu=t['未知损耗'].s4A-t['未知损耗'].sQA,te=t['期末库存'].s4A-t['期末库存'].sQA;
  document.getElementById('cards').innerHTML=`
    <div class="cd"><div class="vl">${{t['销售额'].s4A.toFixed(0)}}</div><div class="lb">v4Σ销售额</div></div>
    <div class="cd"><div class="vl" style="color:${{Math.abs(tp)>100?(tp>0?'#cf1322':'#389e0d'):'#333'}}">${{tp>0?'+':''}}${{tp.toFixed(1)}}</div><div class="lb">Σ毛利差</div></div>
    <div class="cd"><div class="vl" style="color:${{Math.abs(tu)>100?'#cf1322':'#333'}}">${{tu>0?'+':''}}${{tu.toFixed(1)}}</div><div class="lb">Σ未知损耗差</div></div>
    <div class="cd"><div class="vl" style="color:${{Math.abs(te)>1000?'#cf1322':'#333'}}">${{te>0?'+':''}}${{te.toFixed(1)}}</div><div class="lb">Σ期末库存差</div></div>`;
}}
f();
</script></body></html>'''

out = f'/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据/data/output/v4_vs_QDM_final_{DATE}.html'
with open(out, 'w', encoding='utf-8') as f: f.write(html)
print(f'Done: {out} | {len(m)} SKU | v4 profit={m["pft"].sum():.2f} QDM profit={m["q_article_profit_amt"].sum():.2f}')
