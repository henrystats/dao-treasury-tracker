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
def short_address(addr):
    return f"{addr[:6]}...{addr[-4:]}"

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
            st.warning(f"Unexpected token data for {wallet} on {chain}: {data}")
            return []
    except Exception as e:
        st.error(f"Failed to fetch token balances for {wallet} on {chain}: {e}")
        return []

    return [
        {
            "Wallet": wallet,
            "Chain": chain,
            "Token": t.get("optimized_symbol") or t.get("display_symbol") or t.get("symbol"),
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
for wallet in selected_wallets:
    for chain in CHAIN_IDS:
        wallet_data.extend(fetch_token_balances(wallet, chain))

df_wallets = pd.DataFrame(wallet_data)
df_wallets["Chain"] = df_wallets["Chain"].map(CHAIN_NAMES)
df_wallets = df_wallets[df_wallets["Chain"].isin(selected_chains)]

# ============ Fetch DeFi Protocol Positions ============
protocol_data = []
for wallet in selected_wallets:
    protocols = fetch_protocols(wallet)
    for p in protocols:
        for item in p.get("portfolio_item_list", []):
            token_list = item.get("detail", {}).get("supply_token_list", []) + item.get("detail", {}).get("reward_token_list", [])
            for token in token_list:
                protocol_data.append({
                    "Protocol": p.get("name"),
                    "Blockchain": CHAIN_NAMES.get(p.get("chain"), p.get("chain")),
                    "Classification": item.get("name"),
                    "Wallet": wallet,
                    "Token": token.get("optimized_symbol") or token.get("display_symbol") or token.get("symbol"),
                    "Token Balance": token.get("amount"),
                    "USD Value": token.get("amount", 0) * token.get("price", 0)
                })

df_protocols = pd.DataFrame(protocol_data)
df_protocols = df_protocols[df_protocols["Blockchain"].isin(selected_chains)]

# ============ Counters ============
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
for i, (chain, val) in enumerate(top_chains.items()):
    [c3, c4][i].metric(f"{chain}", format_usd(val))
c5.metric("Other Chains", format_usd(other_chains))

# ============ Pie Charts with Plotly ============
st.markdown("### 🔍 Breakdown")
col1, col2 = st.columns(2)

if not chain_sums.empty:
    colors = [COLOR_JSON.get(name, None) for name in chain_sums.index]
    colors = [c for c in colors if c]
    fig1 = px.pie(
        names=chain_sums.index,
        values=chain_sums.values,
        title="Value by Chain",
        hole=0.4
    )
    fig1.update_traces(textinfo="percent+label")
    col1.plotly_chart(fig1, use_container_width=True)

if not df_protocols.empty:
    protocol_sums = df_protocols.groupby("Protocol")["USD Value"].sum().sort_values(ascending=False)
    top10 = protocol_sums.head(10)
    others_sum = protocol_sums.iloc[10:].sum()
    protocol_sums = pd.concat([top10, pd.Series({"Others": others_sum})])

    colors = [COLOR_JSON.get(name, None) for name in protocol_sums.index]
    colors = [c for c in colors if c]
    fig2 = px.pie(
        names=protocol_sums.index,
        values=protocol_sums.values,
        title="Value by Protocol",
        hole=0.4
    )
    fig2.update_traces(textinfo="percent+label")
    col2.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# ============ Wallet Balances ============
st.subheader("💰 Wallet Balances")

if not df_wallets.empty:
    df_wallets = df_wallets.sort_values("USD Value", ascending=False)
    df_wallets["USD Value"] = df_wallets["USD Value"].apply(format_usd)
    df_wallets["Token Balance"] = df_wallets["Token Balance"].apply(lambda x: f"{x:,.4f}")
    df_wallets["Wallet"] = df_wallets["Wallet"].apply(short_address)
    df_wallets = df_wallets[["Wallet", "Chain", "Token", "Token Balance", "USD Value"]]
    st.dataframe(df_wallets, use_container_width=True, hide_index=True)
else:
    st.info("No wallet balances found.")

st.markdown("---")

# ============ DeFi Protocols ============
st.subheader("🏦 DeFi Protocol Positions")

if not df_protocols.empty:
    df_protocols["USD Value"] = df_protocols["USD Value"].apply(format_usd)
    df_protocols["Token Balance"] = df_protocols["Token Balance"].apply(lambda x: f"{x:,.4f}")
    df_protocols["Wallet"] = df_protocols["Wallet"].apply(short_address)

    protocol_order = df_protocols.groupby("Protocol")["USD Value"].apply(lambda x: sum(float(v.strip('$KM')) * (1_000_000 if 'M' in v else 1_000 if 'K' in v else 1) for v in x)).sort_values(ascending=False)

    for protocol in protocol_order.index:
        st.markdown(
            f'<h3 style="display:flex;align-items:center;">'
            f'<img src="{LOGO_URL}" width="24" style="margin-right:10px;">'
            f'{protocol} ({format_usd(protocol_order[protocol])})'
            f'</h3>',
            unsafe_allow_html=True
        )
        protocol_df = df_protocols[df_protocols["Protocol"] == protocol]
        for classification in protocol_df["Classification"].dropna().unique():
            st.markdown(f"### {classification}")
            display_df = protocol_df[protocol_df["Classification"] == classification]
            display_df = display_df[["Wallet", "Blockchain", "Token", "Token Balance", "USD Value"]]
            display_df = display_df.sort_values(by="USD Value", ascending=False)
            st.dataframe(display_df, use_container_width=True, hide_index=True)
else:
    st.info("No DeFi protocol positions found.")
