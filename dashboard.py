import streamlit as st, requests, pandas as pd, plotly.express as px, json, gspread
import datetime, pytz
from google.oauth2.service_account import Credentials

# ──────────────────────────────── CONFIG ─────────────────────────────────────
st.set_page_config(page_title="DeFi Treasury Tracker", layout="wide")
st.title("📊 DeFi Treasury Tracker")

ACCESS_KEY = st.secrets["ACCESS_KEY"]          # Debank Pro key
SHEET_ID   = st.secrets["sheet_id"]            # Google-Sheets ID
SA_INFO    = json.loads(st.secrets["gcp_service_account"])  # service-account JSON

CHAIN_IDS   = ["eth", "arb", "base", "scrl"]
CHAIN_NAMES = {"eth":"Ethereum", "arb":"Arbitrum",
               "base":"Base",    "scrl":"Scroll"}
headers     = {"AccessKey": ACCESS_KEY}

# ─────────────── Static (trimmed) logo + colour dictionaries ────────────────
TOKEN_LOGOS = {
    "ETH":"https://static.debank.com/image/coin/logo_url/eth/6443cdccced33e204d90cb723c632917.png",
    "WETH":"https://static.debank.com/image/eth_token/logo_url/0xc02aaa39.../61844453e63cf81301f845d7864236f6.png",
}
PROTOCOL_LOGOS = {
    "Curve":"https://static.debank.com/image/project/logo_url/curve/aa991be165e771cff87ae61e2a61ef68.png",
    "Aerodrome":"https://static.debank.com/image/project/logo_url/base_aerodrome/f02d753bc321dc8ba480f0424a686482.png",
}
COLOR_JSON = {
    "Ethereum":"#627EEA","Arbitrum":"#28A0F0","Base":"#0052FF","Scroll":"#FEDA03",
    "Curve":"#FF007A","Aave":"#B6509E","Lido":"#00A3FF","Aerodrome":"#1AAB9B",
}

# ─────────────────────────── Google-Sheets helpers ──────────────────────────
def _gc_client():
    creds = Credentials.from_service_account_info(
        SA_INFO,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

@st.cache_data(ttl=600)
def load_wallets() -> list[str]:
    try:
        ws = _gc_client().open_by_key(SHEET_ID).worksheet("addresses")
        w  = [v.strip() for v in ws.col_values(1) if v.strip().startswith("0x")]
        return w or ["0xf40bcc0845528873784F36e5C105E62a93ff7021"]
    except Exception as e:
        st.warning(f"⚠️  Sheets fetch failed, using fallback wallets. ({e})")
        return ["0xf40bcc0845528873784F36e5C105E62a93ff7021"]

WALLETS = load_wallets()

# ───────────────────────────── Helper functions ─────────────────────────────
def first_symbol(t:dict):
    return t.get("optimized_symbol") or t.get("display_symbol") or t.get("symbol")

def token_category(tok:str) -> str:
    t = tok.upper()
    if "ETH" in t:                               return "ETH"
    if any(s in t for s in ("USDC", "USDT")):    return "Stables"
    return tok                                   # keep original

def fmt_usd(v:float)->str:
    return f"${v/1e6:.2f}M" if v>=1e6 else f"${v/1e3:.1f}K" if v>=1e3 else f"${v:.2f}"

def md_table(df:pd.DataFrame, cols:list[str]) -> str:
    hdr="| "+" | ".join(cols)+" |"
    sep="| "+" | ".join("---" for _ in cols)+" |"
    rows=["| "+" | ".join(str(r[c]) for c in cols)+" |" for _,r in df.iterrows()]
    return "\n".join([hdr,sep,*rows])

def link_wallet(a:str): return f"[{a[:6]}…{a[-4:]}](https://debank.com/profile/{a})"

# ───────────────────── Debank API fetchers (10-min cache) ────────────────────
@st.cache_data(ttl=600,show_spinner=False)
def fetch_tokens(wallet:str, chain:str):
    url="https://pro-openapi.debank.com/v1/user/token_list"
    r=requests.get(url,params={"id":wallet,"chain_id":chain,"is_all":False},headers=headers)
    if r.status_code!=200: return []
    out=[]
    for t in r.json():
        price,amt=t.get("price",0),t.get("amount",0)
        if price<=0: continue
        out.append({"Wallet":wallet,
                    "Chain":CHAIN_NAMES.get(chain,chain),
                    "Token":first_symbol(t),
                    "Token Balance":amt,
                    "USD Value":amt*price})
    return out

@st.cache_data(ttl=600,show_spinner=False)
def fetch_protocols(wallet:str):
    url="https://pro-openapi.debank.com/v1/user/all_complex_protocol_list"
    r=requests.get(url,params={"id":wallet,"chain_ids":",".join(CHAIN_IDS)},headers=headers)
    return r.json() if r.status_code==200 else []

# ─────────────────────────────── Build dataframes ────────────────────────────
wallet_rows=[]
for w in WALLETS:
    for cid in CHAIN_IDS:
        wallet_rows += fetch_tokens(w,cid)
df_wallets=pd.DataFrame(wallet_rows)

prot_rows=[]
for w in WALLETS:
    for p in fetch_protocols(w):
        for item in p.get("portfolio_item_list",[]):
            desc=(item.get("detail") or {}).get("description") or ""
            toks=(item.get("detail") or {}).get("supply_token_list",[])+\
                 (item.get("detail") or {}).get("reward_token_list",[])
            for t in toks:
                price,amt=t.get("price",0),t.get("amount",0)
                if price<=0: continue
                sym = desc if desc else first_symbol(t)
                prot_rows.append({"Protocol":p.get("name"),
                                  "Wallet":w,
                                  "USD Value":amt*price,
                                  "Token":sym})
df_protocols=pd.DataFrame(prot_rows)

# ────────────────────────────── Snapshot writer ──────────────────────────────
def write_snapshot():
    if df_protocols.empty and df_wallets.empty: return
    iso_hour=datetime.datetime.utcnow().replace(minute=0,second=0,microsecond=0).isoformat()
    gc=_gc_client(); sh=gc.open_by_key(SHEET_ID)
    try: ws=sh.worksheet("history")
    except gspread.WorksheetNotFound:
        ws=sh.add_worksheet("history",rows=2,cols=4)
        ws.append_row(["timestamp","history_type","name","usd_value"])

    last=ws.get_all_values()[-1] if ws.row_count>1 else []
    if last and last[0]==iso_hour: return

    # protocol rows
    prot_sum=df_protocols.groupby("Protocol")["USD Value"].sum()
    rows=[[iso_hour,"protocol",p,round(v,2)] for p,v in prot_sum.items()]

    # token-category rows
    tok_sum=(df_wallets.assign(cat=df_wallets["Token"].map(token_category))
             .groupby("cat")["USD Value"].sum())
    rows += [[iso_hour,"token",c,round(v,2)] for c,v in tok_sum.items()]

    ws.append_rows(rows, value_input_option="RAW")

@st.cache_data(ttl=3600,show_spinner=False)
def _snapshot_once_hour(): write_snapshot(); return True
_snapshot_once_hour()

# ────────────────────────────  DASHBOARD OUTPUT  ─────────────────────────────
tot_val=df_wallets["USD Value"].sum()+df_protocols["USD Value"].sum()
st.metric("📦 Total Value", fmt_usd(tot_val))

# --- history charts (last 7d) -------------------------------------------------
def load_history():
    try:
        ws=_gc_client().open_by_key(SHEET_ID).worksheet("history")
        hist=pd.DataFrame(ws.get_all_records())
        hist["timestamp"]=pd.to_datetime(hist["timestamp"])
        return hist
    except: return pd.DataFrame(columns=["timestamp","history_type","name","usd_value"])

hist=load_history()
week=hist[hist["timestamp"]>=pd.Timestamp.utcnow()-pd.Timedelta(days=7)]

st.markdown("### 📈 History – last 7 days")
colA,colB=st.columns(2)

if not week.empty:
    # protocol area
    p=week[week["history_type"]=="protocol"]
    if not p.empty:
        top=p.groupby("name")["usd_value"].last().sort_values(ascending=False).head(10).index
        p["name"]=p["name"].where(p["name"].isin(top),"Others")
        colA.plotly_chart(px.area(p,x="timestamp",y="usd_value",color="name",
                                  title="Top Protocols").update_layout(showlegend=True),
                          use_container_width=True)

    # token area
    t=week[week["history_type"]=="token"]
    if not t.empty:
        cats=t.groupby("name")["usd_value"].last().sort_values(ascending=False).head(9).index
        t["name"]=t["name"].where(t["name"].isin(cats),"Others")
        colB.plotly_chart(px.area(t,x="timestamp",y="usd_value",color="name",
                                  title="Token Categories").update_layout(showlegend=True),
                          use_container_width=True)

st.markdown("---")

# --- Wallet balances table ----------------------------------------------------
st.subheader("💰 Wallet Balances")
if not df_wallets.empty:
    df=df_wallets.sort_values("USD Value",ascending=False).copy()
    df["USD Value"]=df["USD Value"].apply(fmt_usd)
    df["Token Balance"]=df["Token Balance"].apply(lambda x:f"{x:,.4f}")
    df["Wallet"]=df["Wallet"].apply(link_wallet)
    df["Token"]=df["Token"].apply(
        lambda t:f'<img src="{TOKEN_LOGOS.get(t,"")}" width="16" style="vertical-align:middle;margin-right:4px;"> {t}')
    st.markdown(md_table(df,["Wallet","Chain","Token","Token Balance","USD Value"]), unsafe_allow_html=True)
else:
    st.info("No wallet balances found.")

# --- Protocol positions table -------------------------------------------------
st.subheader("🏦 DeFi Protocol Positions")
if not df_protocols.empty:
    dfp=df_protocols.copy()
    dfp["USD Value"]=dfp["USD Value"].apply(fmt_usd)
    order=dfp.groupby("Protocol")["USD Value"].apply(
        lambda vs:sum(float(v.strip("$MK"))*(1e6 if v.endswith("M") else 1e3 if v.endswith("K") else 1) for v in vs)
    ).sort_values(ascending=False)

    for proto in order.index:
        st.markdown(
            f'<h3><img src="{PROTOCOL_LOGOS.get(proto,"")}" width="24" '
            f'style="vertical-align:middle;margin-right:6px;">{proto} ({fmt_usd(order[proto])})</h3>',
            unsafe_allow_html=True)
        sub=dfp[dfp["Protocol"]==proto]
        sub["Token"]=sub["Token"].apply(
            lambda t:f'<img src="{TOKEN_LOGOS.get(t,"")}" width="16" style="vertical-align:middle;margin-right:4px;"> {t}')
        st.markdown(md_table(sub[["Wallet","Token","USD Value"]].assign(
            Wallet=sub["Wallet"].apply(link_wallet)), ["Wallet","Token","USD Value"]), unsafe_allow_html=True)
else:
    st.info("No DeFi protocol positions found.")
