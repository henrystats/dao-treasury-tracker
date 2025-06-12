import streamlit as st, requests, pandas as pd, plotly.express as px, json, gspread
import datetime
from google.oauth2.service_account import Credentials

# ───────────────────────── CONFIG ────────────────────────────
st.set_page_config(page_title="DeFi Treasury Tracker", layout="wide")
st.title("📊 DeFi Treasury Tracker")

ACCESS_KEY = st.secrets["ACCESS_KEY"]
SHEET_ID   = st.secrets["sheet_id"]
SA_INFO    = json.loads(st.secrets["gcp_service_account"])

CHAIN_IDS   = ["eth", "arb", "base", "scrl"]
CHAIN_NAMES = {"eth":"Ethereum","arb":"Arbitrum","base":"Base","scrl":"Scroll"}
headers     = {"AccessKey": ACCESS_KEY}

TOKEN_LOGOS = {
    "ETH":"https://static.debank.com/image/coin/logo_url/eth/6443cdccced33e204d90cb723c632917.png",
    "WETH":"https://static.debank.com/image/eth_token/logo_url/0xc02aaa39b223fe.../61844453e63cf81301f845d7864236f6.png",
}
PROTOCOL_LOGOS = {
    "Curve":"https://static.debank.com/image/project/logo_url/curve/aa991be165e771cff87ae61e2a61ef68.png",
    "Aerodrome":"https://static.debank.com/image/project/logo_url/base_aerodrome/f02d753bc321dc8ba480f0424a686482.png",
}
COLOR_JSON = {
    "Ethereum":"#627EEA","Arbitrum":"#28A0F0","Base":"#0052FF","Scroll":"#FEDA03",
    "Curve":"#FF007A","Aave":"#B6509E","Lido":"#00A3FF","Aerodrome":"#1AAB9B",
}

# ──────── CSS for fixed-width tables ────────
st.markdown("""
<style>
table {table-layout:fixed;width:100%}

/* keep long text from overflowing */
th,td {overflow:hidden;text-overflow:ellipsis;white-space:nowrap}

/* new column widths – order: Wallet | Chain | Token | Token Balance | USD Value */
th:nth-child(1), td:nth-child(1) {width:12%}   /* Wallet  */
th:nth-child(2), td:nth-child(2) {width:12%}   /* Chain   */
th:nth-child(3), td:nth-child(3) {width:36%}   /* Token   */
th:nth-child(4), td:nth-child(4) {width:30%}   /* Token Balance */
th:nth-child(5), td:nth-child(5) {width:10%}   /* USD Value */

</style>""", unsafe_allow_html=True)

# ───────────── Google-Sheets helpers ─────────────
def _gc():
    creds = Credentials.from_service_account_info(
        SA_INFO, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

@st.cache_data(ttl=600)
def load_wallets():
    try:
        ws=_gc().open_by_key(SHEET_ID).worksheet("addresses")
        vals=[v.strip() for v in ws.col_values(1) if v.strip().startswith("0x")]
        return vals or ["0xf40bcc0845528873784F36e5C105E62a93ff7021"]
    except Exception as e:
        st.warning(f"⚠️ Sheets fetch failed, using fallback wallet. ({e})")
        return ["0xf40bcc0845528873784F36e5C105E62a93ff7021"]

WALLETS = load_wallets()

# ───────────── helpers ─────────────
def first_symbol(t): return t.get("optimized_symbol") or t.get("display_symbol") or t.get("symbol")
def link_wallet(a):  return f"[{a[:6]}…{a[-4:]}](https://debank.com/profile/{a})"
def fmt_usd(v):      return f"${v/1e6:.2f}M" if v>=1e6 else f"${v/1e3:.1f}K" if v>=1e3 else f"${v:,.0f}"
def token_category(tok:str):
    T=tok.upper()
    if "ETH" in T: return "ETH"
    if any(k in T for k in ("USDC","USDT","DAI","USDE")): return "Stables"
    return "Others"
def md_table(df,cols):
    hdr="| "+" | ".join(cols)+" |"; sep="| "+" | ".join("---" for _ in cols)+" |"
    rows=["| "+" | ".join(str(r[c]) for c in cols)+" |" for _,r in df.iterrows()]
    return "\n".join([hdr,sep,*rows])
def ensure_utc(ts: pd.Timestamp):
    return ts if ts.tzinfo else ts.tz_localize("UTC")

# ───────────── Debank fetchers ─────────────
@st.cache_data(ttl=600, show_spinner=False)
def fetch_tokens(wallet,chain):
    url="https://pro-openapi.debank.com/v1/user/token_list"
    r=requests.get(url,params={"id":wallet,"chain_id":chain,"is_all":False},headers=headers)
    if r.status_code!=200: return []
    out=[]
    for t in r.json():
        price,amt=t.get("price",0),t.get("amount",0)
        if price<=0: continue
        out.append({"Wallet":wallet,"Chain":CHAIN_NAMES.get(chain,chain),
                    "Token":first_symbol(t),"Token Balance":amt,"USD Value":amt*price})
    return out

@st.cache_data(ttl=600, show_spinner=False)
def fetch_protocols(wallet):
    url="https://pro-openapi.debank.com/v1/user/all_complex_protocol_list"
    r=requests.get(url,params={"id":wallet,"chain_ids":",".join(CHAIN_IDS)},headers=headers)
    return r.json() if r.status_code==200 else []

# ───────────── sidebar ─────────────
sel_wallets = st.sidebar.multiselect("Wallets", WALLETS, default=WALLETS)
sel_chains  = st.sidebar.multiselect("Chains",  list(CHAIN_NAMES.values()), default=list(CHAIN_NAMES.values()))

# ───────────── build dfs ─────────────
wallet_rows=[]
for w in sel_wallets:
    for cid in CHAIN_IDS: wallet_rows+=fetch_tokens(w,cid)

df_wallets=pd.DataFrame(wallet_rows)
df_wallets=df_wallets[df_wallets["Chain"].isin(sel_chains)].copy()
df_wallets["USD Value"]=pd.to_numeric(df_wallets["USD Value"],errors="coerce")
df_wallets=df_wallets[df_wallets["USD Value"]>=1]                      # filter <1

prot_rows=[]
for w in sel_wallets:
    for p in fetch_protocols(w):
        for it in p.get("portfolio_item_list",[]):
            desc=(it.get("detail") or {}).get("description") or ""
            toks=(it.get("detail") or {}).get("supply_token_list",[])+\
                 (it.get("detail") or {}).get("reward_token_list",[])
            for t in toks:
                price,amt=t.get("price",0),t.get("amount",0)
                if price<=0: continue
                sym = desc if desc and not desc.startswith("#") else first_symbol(t)
                prot_rows.append({"Protocol":p.get("name"),"Classification":it.get("name",""),
                                  "Blockchain":CHAIN_NAMES.get(p.get("chain"),p.get("chain")),
                                  "Pool": it.get("pool", {}).get("id", ""),
                                  "Wallet":w,"Token":sym,
                                  "Token Balance":amt,"USD Value":amt*price})
df_protocols=pd.DataFrame(prot_rows)
df_protocols=df_protocols[df_protocols["Blockchain"].isin(sel_chains)].copy()
df_protocols["USD Value"]=pd.to_numeric(df_protocols["USD Value"],errors="coerce")
df_protocols=df_protocols[df_protocols["USD Value"]>=1]                # filter <1

# ───────────── snapshot (unchanged) ─────────────
def write_snapshot():
    if df_protocols.empty and df_wallets.empty: return
    hour=datetime.datetime.utcnow().replace(minute=0,second=0,microsecond=0).isoformat()
    gc=_gc(); sh=gc.open_by_key(SHEET_ID)
    try: ws=sh.worksheet("history")
    except gspread.WorksheetNotFound:
        ws=sh.add_worksheet("history",rows=2,cols=4)
        ws.append_row(["timestamp","history_type","name","usd_value"])

    last=ws.get_all_values()[-1] if ws.row_count>1 else []
    if last and last[0]==hour: return

    rows=[[hour,"protocol",p,round(v,2)]
          for p,v in df_protocols.groupby("Protocol")["USD Value"].sum().items()]

    combined=pd.concat([df_wallets[["Token","USD Value"]],
                        df_protocols[["Token","USD Value"]]],ignore_index=True)
    cat_sum=(combined.assign(cat=combined["Token"].map(token_category))
                    .groupby("cat")["USD Value"].sum())
    rows += [[hour,"token",c,round(v,2)] for c,v in cat_sum.items()]
    ws.append_rows(rows,value_input_option="RAW")

@st.cache_data(ttl=3600,show_spinner=False)
def _hourly(): write_snapshot(); return True
_hourly()

# ───────────── counters ─────────────
tot_val  = df_wallets["USD Value"].sum()+df_protocols["USD Value"].sum()
tot_defi = df_protocols["USD Value"].sum()
tot_wal  = df_wallets["USD Value"].sum()
last_ts  = ensure_utc(pd.Timestamp.utcnow()).strftime("%Y-%m-%d %H:%M UTC")

cA,cB,cC,cD = st.columns(4)
cA.metric("📦 Total Value",  fmt_usd(tot_val))
cB.metric("🏦 DeFi Protocols",  fmt_usd(tot_defi))
cC.metric("💰 Wallet Balances", fmt_usd(tot_wal))
elapsed = (
    datetime.datetime.utcnow()
    - ensure_utc(pd.Timestamp.utcnow()).to_pydatetime().replace(tzinfo=None)
).total_seconds()
readable = "just now" if elapsed < 60 else f"{int(elapsed//60)} min ago" if elapsed < 3600 else f"{int(elapsed//3600)} hr ago"
cD.metric("⏱️ Updated", readable)

# ───────────── breakdown pies ─────────────
st.markdown("## 🔍 DAO Treasury Breakdown")
pie1_col, pie2_col = st.columns(2)

# ---------- chain pie ----------
chain_sum = (
    df_wallets.groupby("Chain")["USD Value"].sum()
    + df_protocols.groupby("Blockchain")["USD Value"].sum()
).astype(float).sort_values(ascending=False)

if not chain_sum.empty:
    chain_df = chain_sum.reset_index()
    chain_df.columns = ["chain", "usd"]          # ← robust rename
    fig_chain = px.pie(
        chain_df,
        names="chain",
        values="usd",
        hole=.4,
        color_discrete_sequence=[COLOR_JSON.get(c, "#ccc") for c in chain_df["chain"]]
    )
    fig_chain.update_traces(
        texttemplate="%{label}<br>%{percent}<br>%{customdata}",
        customdata=[fmt_usd(v) for v in chain_df["usd"]],
        hovertemplate="chain = %{label}<br>value = %{customdata}<extra></extra>"
    )
    fig_chain.update_layout(title_text="By Chain")
    pie1_col.plotly_chart(fig_chain, use_container_width=True)

# ---------- protocol pie ----------
if not df_protocols.empty or not df_wallets.empty:
    proto_sum = df_protocols.groupby("Protocol")["USD Value"].sum()
    proto_sum.loc["Wallet Balances"] = df_wallets["USD Value"].sum()
    proto_sum = proto_sum.astype(float).sort_values(ascending=False)
    top5 = proto_sum.head(5)
    if proto_sum.size > 5:
        top5.loc["Others"] = proto_sum.iloc[5:].sum()
    proto_df = top5.reset_index()
    proto_df.columns = ["protocol", "usd"]       # ← robust rename
    fig_proto = px.pie(
        proto_df,
        names="protocol",
        values="usd",
        hole=.4,
        color_discrete_sequence=[COLOR_JSON.get(p, "#ccc") for p in proto_df["protocol"]]
    )
    fig_proto.update_traces(
        texttemplate="%{label}<br>%{percent}<br>%{customdata}",
        customdata=[fmt_usd(v) for v in proto_df["usd"]],
        hovertemplate="protocol = %{label}<br>value = %{customdata}<extra></extra>"
    )
    fig_proto.update_layout(title_text="By DeFi Protocols")
    pie2_col.plotly_chart(fig_proto, use_container_width=True)

st.markdown("---")

# ───────────── history area charts ─────────────
def load_history():
    try:
        ws=_gc().open_by_key(SHEET_ID).worksheet("history")
        h=pd.DataFrame(ws.get_all_records())
        h["usd_value"]=pd.to_numeric(h["usd_value"],errors="coerce")
        h["timestamp"]=pd.to_datetime(h["timestamp"],utc=True,errors="coerce")
        return h.dropna(subset=["timestamp","usd_value"])
    except: return pd.DataFrame(columns=["timestamp","history_type","name","usd_value"])

hist=load_history()

st.markdown("## 📈 Historical Data")
area1,area2=st.columns(2)

if not hist.empty:
    # protocol area
    p=hist[hist["history_type"]=="protocol"].copy()
    if not p.empty:
        p["usd_value"]=pd.to_numeric(p["usd_value"],errors="coerce").fillna(0)
        top=p.groupby("name")["usd_value"].last().nlargest(10).index
        p.loc[~p["name"].isin(top),"name"]="Others"
        fig_p=px.area(p,x="timestamp",y="usd_value",color="name")
        fig_p.update_layout(title="Top Protocols",xaxis_title="Date",yaxis_title="")
        fig_p.update_yaxes(tickformat="$~s")
        area1.plotly_chart(fig_p,use_container_width=True)

    # token area
    t=hist[hist["history_type"]=="token"].copy()
    if not t.empty:
        t["usd_value"]=pd.to_numeric(t["usd_value"],errors="coerce").fillna(0)
        cats=["ETH","Stables","Others"]
        t.loc[~t["name"].isin(cats),"name"]="Others"
        fig_t=px.area(t,x="timestamp",y="usd_value",color="name")
        fig_t.update_layout(title="Token Categories",xaxis_title="Date",yaxis_title="")
        fig_t.update_yaxes(tickformat="$~s")
        area2.plotly_chart(fig_t,use_container_width=True)

st.markdown("---")

# ───────────── wallet table ─────────────
st.subheader("💰 Wallet Balances")
if not df_wallets.empty:
    df=df_wallets.sort_values("USD Value",ascending=False).copy()
    df["USD Value"]=df["USD Value"].apply(fmt_usd)
    df["Token Balance"]=df["Token Balance"].apply(lambda x:f"{x:,.4f}")
    df["Wallet"]=df["Wallet"].apply(link_wallet)
    df["Token"]=df["Token"].apply(
        lambda t:f'<img src="{TOKEN_LOGOS.get(t,"")}" width="16" style="vertical-align:middle;margin-right:4px;"> {t}')
    st.markdown(md_table(df,["Wallet","Chain","Token","Token Balance","USD Value"]),
                unsafe_allow_html=True)
else:
    st.info("No wallet balances found.")

st.markdown("---")   # separator before protocol section

# ───────────── protocol positions table ─────────────
st.subheader("🏦 DeFi Protocol Positions")
if not df_protocols.empty:
    dfp=df_protocols.copy()
    dfp["usd_num"] = dfp["USD Value"]
    dfp_raw = df_protocols.copy()
    dfp["USD Value"]=dfp["USD Value"].apply(fmt_usd)
    dfp["Token Balance"]=dfp["Token Balance"].apply(lambda x:f"{x:,.4f}")
    dfp["Wallet"]=dfp["Wallet"].apply(link_wallet)

    order=dfp.groupby("Protocol")["USD Value"].apply(lambda vs:
        sum(float(v.strip("$MK"))*(1e6 if v.endswith("M") else 1e3 if v.endswith("K") else 1) for v in vs)
    ).sort_values(ascending=False)

    for proto in order.index:
        st.markdown(
            f'<h3><img src="{PROTOCOL_LOGOS.get(proto,"")}" width="24" style="vertical-align:middle;margin-right:6px;">'
            f'{proto} ({fmt_usd(order[proto])})</h3>', unsafe_allow_html=True)

        sub = dfp[dfp["Protocol"] == proto].copy()

        for cls in sub["Classification"].dropna().unique():
            st.markdown(f"<h4 style='margin:6px 0 2px'>{cls}</h4>", unsafe_allow_html=True)

            # ── special handling for Liquidity Pool rows ──
            if cls == "Liquidity Pool" and proto not in ("Pendle", "Pendle V2"):
                raw_lp = dfp_raw[
                    (dfp_raw["Protocol"] == proto) &
                    (dfp_raw["Classification"] == cls)
                ].copy()

                agg_rows = []
                for pid, grp in raw_lp.groupby("Pool"):
                       # ── collapse duplicate tokens (e.g. supply + reward) first ──
                    grp = (
                        grp.groupby("Token", as_index=False)
                           .agg({"USD Value":"sum",
                                 "Token Balance":"sum",
                                 "Wallet":"first",
                                 "Chain":"first"})
                    )
                    usd_total = grp["USD Value"].sum()

                    token_col = " + ".join(
                        f'<img src="{TOKEN_LOGOS.get(t, "")}" '
                        f'width="16" style="vertical-align:middle;margin-right:4px;"> {t}'
                        for t in grp["Token"]
                    )

                    bal_col = " + ".join(
                        f'{b:,.4f} {t}' for t, b in zip(grp["Token"], grp["Token Balance"])
                    )

                    agg_rows.append({
                        "Wallet":  link_wallet(grp["Wallet"].iloc[0]),
                        "Chain":   grp["Blockchain"].iloc[0],
                        "Token":   token_col,
                        "Token Balance": bal_col,
                        "USD Value": usd_total
                    })

                part = pd.DataFrame(agg_rows).sort_values("USD Value", ascending=False)
                part["USD Value"] = part["USD Value"].apply(fmt_usd)

            # ── all other classifications ──
            else:
                part = sub[sub["Classification"] == cls].copy().sort_values("USD Value", ascending=False)
                part = part.rename(columns={"Blockchain": "Chain"})
                part["Token"] = part["Token"].apply(
                    lambda t: f'<img src="{TOKEN_LOGOS.get(t, "")}" width="16" '
                              f'style="vertical-align:middle;margin-right:4px;"> {t}'
                )

            st.markdown(
                md_table(
                    part[["Wallet", "Chain", "Token", "Token Balance", "USD Value"]],
                    ["Wallet", "Chain", "Token", "Token Balance", "USD Value"],
                ),
                unsafe_allow_html=True
            )

        st.markdown("<hr style='margin:1.5em 0'>", unsafe_allow_html=True)

else:
    st.info("No DeFi protocol positions found.")
