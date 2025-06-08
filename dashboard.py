import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# ============ Config & Setup ============
st.set_page_config(page_title="DeFi Treasury Tracker", layout="wide")
st.title("📊 DeFi Treasury Tracker")

# Pull your Debank key from secrets.toml under [debank_api_key]
ACCESS_KEY = st.secrets["ACCESS_KEY"]
WALLETS = [
    "0x86fBaEB3D6b5247F420590D303a6ffC9cd523790",
    "0x46cba1e9b1e5db32da28428f2fb85587bcb785e7",
    "0xf40bcc0845528873784F36e5C105E62a93ff7021",
]
CHAIN_IDS = ["eth", "arb", "base", "scrl"]
CHAIN_NAMES = {
    "eth": "Ethereum",
    "arb": "Arbitrum",
    "base": "Base",
    "scrl": "Scroll",
}
headers = {"AccessKey": ACCESS_KEY}
LOGO_URL = "https://static.debank.com/image/project/logo_url/0x/140b607264f4741133c35eb32c6bc314.png"

COLOR_JSON = {
    "Ethereum": "#627EEA",
    "Arbitrum": "#28A0F0",
    "Base": "#0052FF",
    "Scroll": "#FEDA03",
    "Curve": "#FF007A",
    "Uniswap": "#FF007A",
    "Aave": "#B6509E",
    "Compound": "#00D395",
    "Lido": "#00A3FF",
    "Balancer": "#1E1E1E",
    "Yearn": "#0657F9",
    "Maker": "#1AAB9B",
}

# ============ Helper Functions ============
def format_wallet_link(addr):
    short = f"{addr[:6]}...{addr[-4:]}"
    return f"[{short}](https://debank.com/profile/{addr})"

def format_usd(val):
    if val >= 1_000_000:
        return f"${val/1_000_000:.2f}M"
    elif val >= 1_000:
        return f"${val/1_000:.1f}K"
    return f"${val:.2f}"

def fetch_token_balances(wallet, chain):
    url = "https://pro-openapi.debank.com/v1/user/token_list"
    params = {"id": wallet, "chain_id": chain, "is_all": False}
    res = requests.get(url, params=params, headers=headers)
    try:
        res.raise_for_status()
        data = res.json()
        if not isinstance(data, list):
            return []
    except:
        return []
    return [
        {
            "Wallet": wallet,
            "Chain": CHAIN_NAMES.get(chain, chain),
            "Token": " / ".join(filter(None, [
                t.get("optimized_symbol"),
                t.get("display_symbol"),
                t.get("symbol")
            ])),
            "Token Balance": t["amount"],
            "USD Value": t["amount"] * t.get("price", 0)
        }
        for t in data if t.get("price", 0) > 0
    ]

def fetch_protocols(wallet):
    url = "https://pro-openapi.debank.com/v1/user/all_complex_protocol_list"
    params = {"id": wallet, "chain_ids": ",".join(CHAIN_IDS)}
    return requests.get(url, params=params, headers=headers).json()

# ============ Sidebar Filters ============
selected_wallets = st.sidebar.multiselect("Filter by Wallet", options=WALLETS, default=WALLETS)
selected_chains = st.sidebar.multiselect("Filter by Chain", options=list(CHAIN_NAMES.values()), default=list(CHAIN_NAMES.values()))

# ============ Fetch Wallet Balances ============
wallet_data = []
for w in selected_wallets:
    for c in CHAIN_IDS:
        wallet_data.extend(fetch_token_balances(w, c))
df_wallets = pd.DataFrame(wallet_data)
df_wallets = df_wallets[df_wallets["Chain"].isin(selected_chains)]

# ============ Fetch DeFi Protocol Positions ============
protocol_data = []
for w in selected_wallets:
    for p in fetch_protocols(w):
        for item in p.get("portfolio_item_list", []):
            tokens = item.get("detail", {}).get("supply_token_list", []) + item.get("detail", {}).get("reward_token_list", [])
            for t in tokens:
                protocol_data.append({
                    "Protocol": p.get("name"),
                    "Blockchain": CHAIN_NAMES.get(p.get("chain"), p.get("chain")),
                    "Classification": item.get("name"),
                    "Wallet": w,
                    "Token": " / ".join(filter(None, [
                        t.get("optimized_symbol"),
                        t.get("display_symbol"),
                        t.get("symbol")
                    ])),
                    "Token Balance": t.get("amount", 0),
                    "USD Value": t.get("amount", 0) * t.get("price", 0)
                })
df_protocols = pd.DataFrame(protocol_data)
df_protocols = df_protocols[df_protocols["Blockchain"].isin(selected_chains)]

# ============ Metrics ============
total_usd = df_wallets["USD Value"].sum() + df_protocols["USD Value"].sum()
total_defi = df_protocols["USD Value"].sum()
chain_sums = (
    df_wallets.groupby("Chain")["USD Value"].sum()
      .add(df_protocols.groupby("Blockchain")["USD Value"].sum(), fill_value=0)
      .sort_values(ascending=False)
)
top_chains = chain_sums.head(2)
other_chains = chain_sums[2:].sum()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("📦 Total Value", format_usd(total_usd))
c2.metric("🔄 DeFi Protocols", format_usd(total_defi))
for i,(ch,val) in enumerate(top_chains.items()):
    [c3,c4][i].metric(ch, format_usd(val))
c5.metric("Other Chains", format_usd(other_chains))

# ============ Pie Charts ============
st.markdown("### 🔍 Breakdown")
col1, col2 = st.columns(2)

if not chain_sums.empty:
    fig1 = px.pie(
        names=chain_sums.index,
        values=chain_sums.values,
        title="Value by Chain",
        hole=0.4,
        color_discrete_sequence=[COLOR_JSON.get(ch, "#CCC") for ch in chain_sums.index]
    )
    fig1.update_traces(textinfo="percent+label")
    col1.plotly_chart(fig1, use_container_width=True)

if not df_protocols.empty:
    prot_sums = df_protocols.groupby("Protocol")["USD Value"].sum().sort_values(ascending=False)
    top10 = prot_sums.head(10)
    rest = prot_sums.iloc[10:].sum()
    prot_sums = pd.concat([top10, pd.Series({"Others":rest})])
    fig2 = px.pie(
        names=prot_sums.index,
        values=prot_sums.values,
        title="Value by Protocol",
        hole=0.4,
        color_discrete_sequence=[COLOR_JSON.get(p, "#CCC") for p in prot_sums.index]
    )
    fig2.update_traces(textinfo="percent+label")
    col2.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# ============ Tables (Markdown) ============
st.subheader("💰 Wallet Balances")
if not df_wallets.empty:
    df = df_wallets.sort_values("USD Value", ascending=False).copy()
    df["USD Value"] = df["USD Value"].apply(format_usd)
    df["Token Balance"] = df["Token Balance"].apply(lambda x: f"{x:,.4f}")
    df["Wallet"] = df["Wallet"].apply(format_wallet_link)
    st.markdown(df[["Wallet","Chain","Token","Token Balance","USD Value"]]
                .to_markdown(index=False), unsafe_allow_html=True)
else:
    st.info("No wallet balances found.")

st.subheader("🏦 DeFi Protocol Positions")
if not df_protocols.empty:
    dfp = df_protocols.copy()
    dfp["USD Value"] = dfp["USD Value"].apply(format_usd)
    dfp["Token Balance"] = dfp["Token Balance"].apply(lambda x: f"{x:,.4f}")
    dfp["Wallet"] = dfp["Wallet"].apply(format_wallet_link)

    # order protocols by total USD
    order = dfp.groupby("Protocol")["USD Value"]\
               .apply(lambda vs: sum(
                   float(v.strip("$MK"))*(1e6 if v.endswith("M") else 1e3 if v.endswith("K") else 1)
                   for v in vs
               )).sort_values(ascending=False)

    for proto in order.index:
        total = order[proto]
        st.markdown(
            f'<h3><img src="{LOGO_URL}" width="24" style="vertical-align:middle;"> '
            f'{proto} ({format_usd(total)})</h3>',
            unsafe_allow_html=True
        )
        sub = dfp[dfp["Protocol"]==proto]
        for cls in sub["Classification"].unique():
            st.markdown(f"### {cls}")
            part = sub[sub["Classification"]==cls]\
                   .sort_values("USD Value", ascending=False)
            st.markdown(part[["Wallet","Blockchain","Token","Token Balance","USD Value"]]
                        .to_markdown(index=False), unsafe_allow_html=True)
else:
    st.info("No DeFi protocol positions found.")


