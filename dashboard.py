import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# ============ Config & Setup ============
st.set_page_config(page_title="DeFi Treasury Tracker", layout="wide")
st.title("📊 DeFi Treasury Tracker")

ACCESS_KEY = st.secrets["ACCESS_KEY"]
WALLETS = [
    "0x86fBaEB3D6b5247F420590D303a6ffC9cd523790",
    "0x46cba1e9b1e5db32da28428f2fb85587bcb785e7",
    "0xf40bcc0845528873784F36e5C105E62a93ff7021",
]
CHAIN_IDS  = ["eth", "arb", "base", "scrl"]
CHAIN_NAMES= {"eth":"Ethereum", "arb":"Arbitrum", "base":"Base", "scrl":"Scroll"}
headers    = {"AccessKey": ACCESS_KEY}

# ============ Static logo maps (trimmed list—add more as needed) =============
TOKEN_LOGOS = {
    "GHO":"https://static.debank.com/image/eth_token/logo_url/0x40d1...42.png",
    "AAVE":"https://static.debank.com/image/eth_token/logo_url/0x7fc6...21.png",
    "PENDLE":"https://static.debank.com/image/eth_token/logo_url/0x8085...ad.png",
    "ETH":"https://static.debank.com/image/coin/logo_url/eth/6443cdccced33e204d90cb723c632917.png",
    "WETH":"https://static.debank.com/image/eth_token/logo_url/0xc02a...f6.png",
    # … add the rest from your JSON …
}

PROTOCOL_LOGOS = {
    "Aave V3":"https://static.debank.com/image/project/logo_url/aave3/54df7839ab09493ba7540ab832590255.png",
    "Aerodrome":"https://static.debank.com/image/project/logo_url/base_aerodrome/f02d753bc321dc8ba480f0424a686482.png",
    "Curve":"https://static.debank.com/image/project/logo_url/curve/aa991be165e771cff87ae61e2a61ef68.png",
    "ether.fi":"https://static.debank.com/image/project/logo_url/etherfi/6c3ea6e8f02322fa9b417e0726978c41.png",
    # … add the rest …
}

COLOR_JSON = {
    "Ethereum":"#627EEA", "Arbitrum":"#28A0F0", "Base":"#0052FF", "Scroll":"#FEDA03",
    "Curve":"#FF007A", "Aave":"#B6509E", "Lido":"#00A3FF", "Aerodrome":"#1AAB9B",
}

# ============ Helper Functions ===============================================
def first_symbol(tok:dict) -> str:
    """Return optimized_symbol, else display_symbol, else symbol."""
    return tok.get("optimized_symbol") or tok.get("display_symbol") or tok.get("symbol")

def format_wallet_link(addr:str) -> str:
    return f"[{addr[:6]}...{addr[-4:]}](https://debank.com/profile/{addr})"

def format_usd(val:float) -> str:
    if val >= 1_000_000: return f"${val/1_000_000:.2f}M"
    if val >= 1_000:     return f"${val/1_000:.1f}K"
    return f"${val:.2f}"

@st.cache_data(ttl=600, show_spinner=False)
def fetch_token_balances(wallet:str, chain:str):
    url     = "https://pro-openapi.debank.com/v1/user/token_list"
    params  = {"id":wallet, "chain_id":chain, "is_all":False}
    r       = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    data    = r.json() if isinstance(r.json(), list) else []

    rows = []
    for t in data:
        price = t.get("price", 0)
        if price <= 0: continue
        rows.append({
            "Wallet": wallet,
            "Chain":  CHAIN_NAMES.get(chain, chain),
            "Token":  first_symbol(t),
            "Token Balance": t["amount"],
            "USD Value":     t["amount"] * price,
        })
    return rows

@st.cache_data(ttl=600, show_spinner=False)
def fetch_protocols(wallet:str):
    url    = "https://pro-openapi.debank.com/v1/user/all_complex_protocol_list"
    params = {"id":wallet, "chain_ids":",".join(CHAIN_IDS)}
    r      = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    return r.json()

def df_to_markdown(df:pd.DataFrame, cols:list[str]) -> str:
    header = "| " + " | ".join(cols) + " |"
    sep    = "| " + " | ".join("---" for _ in cols) + " |"
    rows   = ["| " + " | ".join(str(r[c]) for c in cols) + " |" for _,r in df.iterrows()]
    return "\n".join([header, sep, *rows])

# ============ Sidebar Filters ================================================
sel_wallets = st.sidebar.multiselect("Filter by Wallet", WALLETS, default=WALLETS)
sel_chains  = st.sidebar.multiselect("Filter by Chain",  list(CHAIN_NAMES.values()), default=list(CHAIN_NAMES.values()))

# ============ Fetch Wallet Balances ==========================================
wallet_rows=[]
for w in sel_wallets:
    for cid in CHAIN_IDS:
        wallet_rows += fetch_token_balances(w, cid)
df_wallets = pd.DataFrame(wallet_rows)
df_wallets = df_wallets[df_wallets["Chain"].isin(sel_chains)]

# ============ Fetch DeFi Positions ===========================================
prot_rows=[]
for w in sel_wallets:
    for p in fetch_protocols(w):
        for item in p.get("portfolio_item_list", []):
            desc  = (item.get("detail") or {}).get("description") or ""
            tokens= (item.get("detail") or {}).get("supply_token_list", []) + \
                    (item.get("detail") or {}).get("reward_token_list", [])
            for t in tokens:
                price = t.get("price", 0)
                if price <= 0: continue
                symbol = desc if desc else first_symbol(t)
                prot_rows.append({
                    "Protocol": p.get("name"),
                    "Blockchain": CHAIN_NAMES.get(p.get("chain"), p.get("chain")),
                    "Classification": item.get("name"),
                    "Wallet": w,
                    "Token": symbol,
                    "Token Balance": t.get("amount",0),
                    "USD Value": t.get("amount",0) * price,
                })
df_protocols = pd.DataFrame(prot_rows)
df_protocols = df_protocols[df_protocols["Blockchain"].isin(sel_chains)]

# ============ Metrics ========================================================
total_usd  = df_wallets["USD Value"].sum() + df_protocols["USD Value"].sum()
total_defi = df_protocols["USD Value"].sum()

chain_sums = (
    df_wallets.groupby("Chain")["USD Value"].sum()
      .add(df_protocols.groupby("Blockchain")["USD Value"].sum(), fill_value=0)
      .sort_values(ascending=False)
)
top2, others = chain_sums.head(2), chain_sums[2:].sum()

c1,c2,c3,c4,c5 = st.columns(5)
c1.metric("📦 Total Value",    format_usd(total_usd))
c2.metric("🔄 DeFi Protocols", format_usd(total_defi))
for i,(ch,val) in enumerate(top2.items()):
    [c3,c4][i].metric(ch, format_usd(val))
c5.metric("Other Chains", format_usd(others))

# ============ Pie Charts =====================================================
st.markdown("### 🔍 Breakdown")
col1,col2 = st.columns(2)

if not chain_sums.empty:
    fig1 = px.pie(
        names=chain_sums.index, values=chain_sums.values, hole=0.4,
        color_discrete_sequence=[COLOR_JSON.get(ch,"#CCC") for ch in chain_sums.index]
    )
    fig1.update_traces(textinfo="percent+label")
    col1.plotly_chart(fig1, use_container_width=True)

if not df_protocols.empty:
    ps = df_protocols.groupby("Protocol")["USD Value"].sum().sort_values(ascending=False)
    ps = pd.concat([ps.head(10), pd.Series({"Others": ps.iloc[10:].sum()})])
    fig2 = px.pie(
        names=ps.index, values=ps.values, hole=0.4,
        color_discrete_sequence=[COLOR_JSON.get(p,"#CCC") for p in ps.index]
    )
    fig2.update_traces(textinfo="percent+label")
    col2.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# ============ Wallet Balances Table ==========================================
st.subheader("💰 Wallet Balances")
if not df_wallets.empty:
    df = df_wallets.sort_values("USD Value", ascending=False).copy()
    df["USD Value"]     = df["USD Value"].apply(format_usd)
    df["Token Balance"] = df["Token Balance"].apply(lambda x:f"{x:,.4f}")
    df["Wallet"]        = df["Wallet"].apply(format_wallet_link)
    df["Token"] = df["Token"].apply(
        lambda t: f'<img src="{TOKEN_LOGOS.get(t,"")}" width="16" style="vertical-align:middle;margin-right:4px;"> {t}'
    )
    st.markdown(df_to_markdown(df, ["Wallet","Chain","Token","Token Balance","USD Value"]), unsafe_allow_html=True)
else:
    st.info("No wallet balances found.")

# ============ DeFi Protocols Table ===========================================
st.subheader("🏦 DeFi Protocol Positions")
if not df_protocols.empty:
    dfp = df_protocols.copy()
    dfp["USD Value"]     = dfp["USD Value"].apply(format_usd)
    dfp["Token Balance"] = dfp["Token Balance"].apply(lambda x:f"{x:,.4f}")
    dfp["Wallet"]        = dfp["Wallet"].apply(format_wallet_link)

    order = dfp.groupby("Protocol")["USD Value"].apply(
        lambda vs: sum(float(v.strip("$MK"))*(1e6 if v.endswith("M") else 1e3 if v.endswith("K") else 1) for v in vs)
    ).sort_values(ascending=False)

    for proto in order.index:
        total = order[proto]
        st.markdown(
            f'<h3><img src="{PROTOCOL_LOGOS.get(proto,"")}" width="24" '
            f'style="vertical-align:middle;margin-right:8px;">'
            f'{proto} ({format_usd(total)})</h3>', unsafe_allow_html=True
        )
        sub = dfp[dfp["Protocol"]==proto]
        for cls in sub["Classification"].unique():
            st.markdown(f"### {cls}")
            part = sub[sub["Classification"]==cls].sort_values("USD Value", ascending=False)
            part["Token"] = part["Token"].apply(
                lambda t: f'<img src="{TOKEN_LOGOS.get(t,"")}" width="16" style="vertical-align:middle;margin-right:4px;"> {t}'
            )
            st.markdown(df_to_markdown(part, ["Wallet","Blockchain","Token","Token Balance","USD Value"]), unsafe_allow_html=True)
else:
    st.info("No DeFi protocol positions found.")
