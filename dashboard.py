import streamlit as st, requests, pandas as pd, plotly.express as px
import json, gspread
from google.oauth2.service_account import Credentials

# ============ Config & Setup ============
st.set_page_config(page_title="DeFi Treasury Tracker", layout="wide")
st.title("📊 DeFi Treasury Tracker")

ACCESS_KEY = st.secrets["ACCESS_KEY"]

# ------------------------------------------------------------------ #
# —— 1️⃣  Fetch wallet addresses from Google Sheets (fallback static) #
# ------------------------------------------------------------------ #
def load_wallets() -> list[str]:
    try:
        creds_info = json.loads(st.secrets["gcp_service_account"])
        creds      = Credentials.from_service_account_info(
            creds_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        gc   = gspread.authorize(creds)
        sh   = gc.open_by_key(st.secrets["sheet_id"])
        ws   = sh.worksheet("addresses")  # tab name
        vals = ws.col_values(1)           # first column
        wallets = [v.strip() for v in vals if v.strip().startswith("0x")]
        if wallets:
            return wallets
    except Exception as e:
        st.warning(f"⚠️  Sheets fetch failed, using default wallets. ({e})")
    return [
        "0xf40bcc0845528873784F36e5C105E62a93ff7021",
    ]

WALLETS = load_wallets()

CHAIN_IDS  = ["eth", "arb", "base", "scrl"]
CHAIN_NAMES= {"eth":"Ethereum", "arb":"Arbitrum", "base":"Base", "scrl":"Scroll"}
headers    = {"AccessKey": ACCESS_KEY}

# ───── ( logo dicts & colors – unchanged ) ─────────────────────────
TOKEN_LOGOS = {
    "GHO":"https://static.debank.com/image/eth_token/logo_url/0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f/1fd570eeab44b1c7afad2e55b5545c42.png",
    "AAVE":"https://static.debank.com/image/eth_token/logo_url/0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9/7baf403c819f679dc1c6571d9d978f21.png",
    "PENDLE":"https://static.debank.com/image/eth_token/logo_url/0x808507121b80c02388fad14726482e061b8da827/b9351f830cd0a6457e489b8c685f29ad.png",
    "ETH":"https://static.debank.com/image/coin/logo_url/eth/6443cdccced33e204d90cb723c632917.png",
    "WETH":"https://static.debank.com/image/eth_token/logo_url/0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2/61844453e63cf81301f845d7864236f6.png",
}
PROTOCOL_LOGOS = {
    "Aave V3":"https://static.debank.com/image/project/logo_url/aave3/54df7839ab09493ba7540ab832590255.png",
    "Aerodrome":"https://static.debank.com/image/project/logo_url/base_aerodrome/f02d753bc321dc8ba480f0424a686482.png",
    "Curve":"https://static.debank.com/image/project/logo_url/curve/aa991be165e771cff87ae61e2a61ef68.png",
    "ether.fi":"https://static.debank.com/image/project/logo_url/etherfi/6c3ea6e8f02322fa9b417e0726978c41.png",
}
COLOR_JSON = {
    "Ethereum":"#627EEA","Arbitrum":"#28A0F0","Base":"#0052FF","Scroll":"#FEDA03",
    "Curve":"#FF007A","Aave":"#B6509E","Lido":"#00A3FF","Aerodrome":"#1AAB9B",
}

# ───── Helper functions (unchanged except first_symbol) ────────────
def first_symbol(tok:dict)->str:
    return tok.get("optimized_symbol") or tok.get("display_symbol") or tok.get("symbol")

def format_wallet_link(addr:str)->str:
    return f"[{addr[:6]}...{addr[-4:]}](https://debank.com/profile/{addr})"

def format_usd(v:float)->str:
    return f"${v/1_000_000:.2f}M" if v>=1e6 else f"${v/1_000:.1f}K" if v>=1e3 else f"${v:.2f}"

@st.cache_data(ttl=600, show_spinner=False)
def fetch_token_balances(wallet:str, chain:str):
    url="https://pro-openapi.debank.com/v1/user/token_list"
    r = requests.get(url, params={"id":wallet,"chain_id":chain,"is_all":False}, headers=headers)
    if r.status_code!=200: return []
    rows=[]
    for t in r.json():
        price=t.get("price",0);   amt=t.get("amount",0)
        if price<=0: continue
        rows.append({
            "Wallet":wallet,"Chain":CHAIN_NAMES.get(chain,chain),
            "Token":first_symbol(t),
            "Token Balance":amt,"USD Value":amt*price})
    return rows

@st.cache_data(ttl=600, show_spinner=False)
def fetch_protocols(wallet:str):
    url="https://pro-openapi.debank.com/v1/user/all_complex_protocol_list"
    r=requests.get(url,params={"id":wallet,"chain_ids":",".join(CHAIN_IDS)},headers=headers)
    return r.json() if r.status_code==200 else []

def df_to_md(df:pd.DataFrame,cols:list[str])->str:
    hdr="| "+" | ".join(cols)+" |"
    sep="| "+" | ".join("---" for _ in cols)+" |"
    rows=["| "+" | ".join(str(r[c]) for c in cols)+" |" for _,r in df.iterrows()]
    return "\n".join([hdr,sep,*rows])

# ============ Sidebar ========================================================
sel_wallets=st.sidebar.multiselect("Wallets",WALLETS,default=WALLETS)
sel_chains =st.sidebar.multiselect("Chains", list(CHAIN_NAMES.values()), default=list(CHAIN_NAMES.values()))

# ============ Fetch data =====================================================
wallet_rows=[]
for w in sel_wallets:
    for cid in CHAIN_IDS:
        wallet_rows+=fetch_token_balances(w,cid)
df_wallets=pd.DataFrame(wallet_rows)
df_wallets=df_wallets[df_wallets["Chain"].isin(sel_chains)]

prot_rows=[]
for w in sel_wallets:
    for p in fetch_protocols(w):
        for it in p.get("portfolio_item_list",[]):
            desc=(it.get("detail") or {}).get("description") or ""
            toks=(it.get("detail") or {}).get("supply_token_list",[])+\
                 (it.get("detail") or {}).get("reward_token_list",[])
            for t in toks:
                price=t.get("price",0); amt=t.get("amount",0)
                if price<=0: continue
                sym=desc if desc else first_symbol(t)
                prot_rows.append({
                    "Protocol":p.get("name"),
                    "Blockchain":CHAIN_NAMES.get(p.get("chain"),p.get("chain")),
                    "Classification":it.get("name"),"Wallet":w,
                    "Token":sym,"Token Balance":amt,"USD Value":amt*price})
df_protocols=pd.DataFrame(prot_rows)
df_protocols=df_protocols[df_protocols["Blockchain"].isin(sel_chains)]

# ============ Metrics ========================================================
tot_usd=df_wallets["USD Value"].sum()+df_protocols["USD Value"].sum()
tot_defi=df_protocols["USD Value"].sum()
chain_sums=(df_wallets.groupby("Chain")["USD Value"].sum()
           +df_protocols.groupby("Blockchain")["USD Value"].sum()).sort_values(ascending=False)
top2,others=chain_sums.head(2),chain_sums[2:].sum()

c1,c2,c3,c4,c5=st.columns(5)
c1.metric("📦 Total Value",format_usd(tot_usd))
c2.metric("🔄 DeFi Protocols",format_usd(tot_defi))
for i,(ch,val) in enumerate(top2.items()): [c3,c4][i].metric(ch,format_usd(val))
c5.metric("Other Chains",format_usd(others))

# ============ Pie charts =====================================================
st.markdown("### 🔍 Breakdown")
col1,col2=st.columns(2)
if not chain_sums.empty:
    col1.plotly_chart(px.pie(names=chain_sums.index,values=chain_sums.values,hole=.4,
        color_discrete_sequence=[COLOR_JSON.get(c,"#ccc") for c in chain_sums.index])
        .update_traces(textinfo="percent+label"), use_container_width=True)

if not df_protocols.empty:
    ps=df_protocols.groupby("Protocol")["USD Value"].sum().sort_values(ascending=False)
    ps=pd.concat([ps.head(10),pd.Series({"Others":ps.iloc[10:].sum()})])
    col2.plotly_chart(px.pie(names=ps.index,values=ps.values,hole=.4,
        color_discrete_sequence=[COLOR_JSON.get(p,"#ccc") for p in ps.index])
        .update_traces(textinfo="percent+label"), use_container_width=True)

st.markdown("---")

# ============ Wallet table ===================================================
st.subheader("💰 Wallet Balances")
if not df_wallets.empty:
    df=df_wallets.sort_values("USD Value",ascending=False).copy()
    df["USD Value"]=df["USD Value"].apply(format_usd)
    df["Token Balance"]=df["Token Balance"].apply(lambda x:f"{x:,.4f}")
    df["Wallet"]=df["Wallet"].apply(format_wallet_link)
    df["Token"]=df["Token"].apply(lambda t:f'<img src="{TOKEN_LOGOS.get(t,"")}" width="16" style="vertical-align:middle;margin-right:4px;"> {t}')
    st.markdown(df_to_md(df,["Wallet","Chain","Token","Token Balance","USD Value"]),unsafe_allow_html=True)
else: st.info("No wallet balances found.")

# ============ Protocols table ===============================================
st.subheader("🏦 DeFi Protocol Positions")
if not df_protocols.empty:
    dfp=df_protocols.copy()
    dfp["USD Value"]=dfp["USD Value"].apply(format_usd)
    dfp["Token Balance"]=dfp["Token Balance"].apply(lambda x:f"{x:,.4f}")
    dfp["Wallet"]=dfp["Wallet"].apply(format_wallet_link)

    order=dfp.groupby("Protocol")["USD Value"].apply(
        lambda vs:sum(float(v.strip("$MK"))*(1e6 if v.endswith("M") else 1e3 if v.endswith("K") else 1) for v in vs)
    ).sort_values(ascending=False)

    for proto in order.index:
        tot=order[proto]
        st.markdown(f'<h3><img src="{PROTOCOL_LOGOS.get(proto,"")}" width="24" style="vertical-align:middle;margin-right:8px;">'
                    f'{proto} ({format_usd(tot)})</h3>', unsafe_allow_html=True)
        sub=dfp[dfp["Protocol"]==proto]
        for cls in sub["Classification"].unique():
            st.markdown(f"### {cls}")
            part=sub[sub["Classification"]==cls].sort_values("USD Value",ascending=False)
            part["Token"]=part["Token"].apply(lambda t:f'<img src="{TOKEN_LOGOS.get(t,"")}" width="16" style="vertical-align:middle;margin-right:4px;"> {t}')
            st.markdown(df_to_md(part,["Wallet","Blockchain","Token","Token Balance","USD Value"]),unsafe_allow_html=True)
else:
    st.info("No DeFi protocol positions found.")

