import streamlit as st, requests, pandas as pd, plotly.express as px, json, gspread
import datetime, time, re, requests_cache, itertools
from google.oauth2.service_account import Credentials
from functools import lru_cache  

requests_cache.install_cache(
    "debank_cache",                                
    expire_after = datetime.timedelta(minutes=15), 
    allowable_methods = ("GET",),                  
    allowable_codes   = (200,),                   
    cache_control     = True,                     
)

# ───────────────────────── CONFIG ────────────────────────────
st.set_page_config(page_title="liquidETH Vault Positions", layout="wide")
st.title("📊 liquidETH Vault Positions")

ACCESS_KEY = st.secrets["ACCESS_KEY"]
SHEET_ID   = st.secrets["sheet_id"]
WALLET_SHEET = "liquid_vaults_wallet_balances"       
SA_INFO    = json.loads(st.secrets["gcp_service_account"])

CHAIN_IDS   = ["eth", "arb", "base", "scrl", "avax", "era", "bsc", "op", "linea", "corn", "zircuit", "bera", "blast", "swell", "uni", "sonic", "hyper","katana","plasma"]
CHAIN_NAMES = {"eth":"Ethereum","arb":"Arbitrum","base":"Base","scrl":"Scroll","avax":"Avalanche","era":"zkSync Era","bsc":"BNB Chain","op":"Optimism",
                "linea":"Linea","corn":"Corn","zircuit":"Zircuit","bera":"Berachain","blast":"Blast","swell":"SwellChain","uni":"Unichain",
               "sonic":"Sonic","hyper":"Hyperliquid","katana":"Katana","plasma":"Plasma"
            }
headers     = {"AccessKey": ACCESS_KEY}

TOKEN_LOGOS = {
    "GHO": "https://static.debank.com/image/eth_token/logo_url/0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f/1fd570eeab44b1c7afad2e55b5545c42.png",
    "AAVE": "https://static.debank.com/image/eth_token/logo_url/0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9/7baf403c819f679dc1c6571d9d978f21.png",
    "PENDLE": "https://static.debank.com/image/eth_token/logo_url/0x808507121b80c02388fad14726482e061b8da827/b9351f830cd0a6457e489b8c685f29ad.png",
    "SSV": "https://static.debank.com/image/eth_token/logo_url/0x9d65ff81a3c488d585bbfb0bfe3c7707c7917f54/2a91cd614844f7c9596c6fbcff85c57c.png",
    "USDC": "https://static.debank.com/image/arb_token/logo_url/0xaf88d065e77c8cc2239327c5edb3a432268e5831/fffcd27b9efff5a86ab942084c05924d.png",
    "weETH": "https://static.debank.com/image/eth_token/logo_url/0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee/6c02f6b3bcd264d433c3676100ad8da6.png",
    "ARB": "https://static.debank.com/image/arb_token/logo_url/0x912ce59144191c1204e64559fe8253a0e49e6548/7623afc27299327fdb0b090fd67e8ff4.png",
    "TANGO": "https://static.debank.com/image/arb_token/logo_url/0xc760f9782f8cea5b06d862574464729537159966/caa8d2a11e12302dd5db9b4496cf849c.png",
    "BASED (unknown)": "https://static.debank.com/image/base_token/logo_url/0x07d15798a67253d76cea61f0ea6f57aedc59dffb/892d5d2a0dafb79d97e451760536f528.png",
    "BRIAN": "https://static.debank.com/image/base_token/logo_url/0x3ecced5b416e58664f04a39dd18935eb71d33b15/5ba04e3a667df4d5a7c4cc4423015663.png",
    "WETH": "https://static.debank.com/image/swell_token/logo_url/swell/48bfb74adddd170e936578aec422836d.png",
    "SNL": "https://static.debank.com/image/base_token/logo_url/0xc5a861787f3e173f2b004d5cfa6a717f5dc5484d/c5f500748fb1f999384eccdfc08e0b4f.png",
    "cbBTC": "https://static.debank.com/image/base_token/logo_url/0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf/a4ae837a6ca2fc45f07a74898cc4ba45.png",
    "XOLO": "https://static.debank.com/image/base_token/logo_url/0xf7cebf7df90a1c59fd0347c41b983846ca8bc76a/51d20d495ea3d5980a1fdbdb38f8515d.png",
    "WCT": "https://static.debank.com/image/op_token/logo_url/0xef4461891dfb3ac8572ccf7c794664a8dd927945/542a47c09c9a133f5a428f17fee817b9.png",
    "MOVE": "https://static.debank.com/image/eth_token/logo_url/0x3073f7aaa4db83f95e9fff17424f71d4751a3073/6d2af9409a5e10490b694fbb3ad0a854.png",
    "USDe": "https://static.debank.com/image/eth_token/logo_url/0x4c9edd5852cd905f086c759e8383e09bff1e68b3/1228d6e73f70f37ec1f6fe02a3bbe6ff.png",
    "ZKJ": "https://static.debank.com/image/eth_token/logo_url/0xc71b5f631354be6853efe9c3ab6b9590f8302e81/840fb61af52d5ffb9cc3ecabbf6b8eba.png",
    "ETHFI": "https://static.debank.com/image/eth_token/logo_url/0xfe0c30065b384f05761f15d0cc899d4f9f9cc0eb/51f0cde8e655b6dd4cc39f2495b60c1e.png",
    "ETH": "https://static.debank.com/image/coin/logo_url/eth/6443cdccced33e204d90cb723c632917.png",
    "USDC(Bridged)": "https://static.debank.com/image/avax_token/logo_url/0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664/c1503ade9d53497fe93ca9f2723c56a1.png",
    "LWFI (unknown)": "https://static.debank.com/image/base_token/logo_url/0x2953ca1174b41b28439ef1155235fa699d7b75ba/906f45d8fadf689ebb38708780f057df.png",
    "9MM": "https://static.debank.com/image/base_token/logo_url/0x340c070260520ae477b88caa085a33531897145b/1e66cba98b954bade18a7678399575ba.png",
    "WAZ": "https://static.debank.com/image/base_token/logo_url/0x5efc4446e1d772428adbd6053a0421ca7b3ed85a/5fc0ecbd399a1cb8a4c212f3b5e1183f.png",
    "SKYA": "https://static.debank.com/image/base_token/logo_url/0x623cd3a3edf080057892aaf8d773bbb7a5c9b6e9/b82923ee85a1f6477eed780357b3484c.png",
    "WIF": "https://static.debank.com/image/base_token/logo_url/0x7f6f6720a73c0f54f95ab343d7efeb1fa991f4f7/f471e8295e99101458756f0b3b25089a.png",
    "Blue": "https://static.debank.com/image/base_token/logo_url/0x891502ba08132653151f822a3a430198f1844115/daf0928bc9af7973d38f7023048dc7f5.png",
    "GOOFS": "https://static.debank.com/image/base_token/logo_url/0x8e5c04f82d6464b420e2018362e7e7ab813cf190/d77975b1e290f3738bc137451be6f63a.png",
    "BDOGE": "https://static.debank.com/image/base_token/logo_url/0xb3ecba1330fe26bb36f40344992c481c2c916f23/2de0ba94bf560c048c838c114313e0e1.png",
    "USA(BASED USA)": "https://static.debank.com/image/base_token/logo_url/0xb56d0839998fd79efcd15c27cf966250aa58d6d3/68b9dd7a75a01feb4fb33e095bdbefe7.png",
    "toby": "https://static.debank.com/image/base_token/logo_url/0xb8d98a102b0079b69ffbc760c8d857a31653e56e/82bdb31b9d8feb9343fda93d84496666.png",
    "BOOMER (baseboomer.com)": "https://static.debank.com/image/base_token/logo_url/0xcde172dc5ffc46d228838446c57c1227e0b82049/cfe0b2eaae96b4cffb5f69fc5a4e0494.png",
    "GURMY": "https://static.debank.com/image/base_token/logo_url/0xd8c3590204d9442a5746f45f3d1e2befea147da0/c91fcf2adbe38e67c20f2599497d710f.png",
    "ITRUMP": "https://static.debank.com/image/base_token/logo_url/0xf389724b320ea4dbc03b8122d61052e1284a65dc/09722fea849ef563b0c3ef8f8a1c2d36.png",
    "WGC": "https://static.debank.com/image/base_token/logo_url/0xfb18511f1590a494360069f3640c27d55c2b5290/e4e4e71f748f382b923eba77319f57cc.png",
    "Boe": "https://static.debank.com/image/base_token/logo_url/0xff62ddfa80e513114c3a0bf4d6ffff1c1d17aadf/d00693431917e61b8212faa500fd0040.png",
    "WBTC": "https://static.debank.com/image/eth_token/logo_url/0x2260fac5e5542a773aa44fbcfedf7c193bc2c599/d3c52e7c7449afa8bd4fad1c93f50d93.png",
    "NURI": "https://static.debank.com/image/scrl_token/logo_url/0xaaae8378809bb8815c08d3c59eb0c7d1529ad769/a8d3cd4269ca15424452a19e7cad8606.png",
    "SCR": "https://static.debank.com/image/scrl_token/logo_url/0xd29687c813d741e2f938f4ac377128810e217b1b/0eabb4976c99ba1e66fdb9b3e5139dcf.png",
    "weETH(layerzero)": "https://static.debank.com/image/eth_token/logo_url/0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee/6c02f6b3bcd264d433c3676100ad8da6.png",
    "LARRY": "https://static.debank.com/image/op_token/logo_url/0xad984fbd3fb10d0b47d561be7295685af726fdb3/90e2cbc96bad26f4d61dde311f8e5f9c.png",
    "HIM": "https://static.debank.com/image/bera_token/logo_url/0x047b41a14f0bef681b94f570479ae7208e577a0c/72f22b3d1ef38a5b660be2863f58c7f4.png",
    "BTC(meme)": "https://static.debank.com/image/bera_token/logo_url/0x0d9ac083dd2760943f773e70ebffe621e950871c/7b0559804701886be6973080a7a5d7a0.png",
    "BERA": "https://static.debank.com/image/bera_token/logo_url/bera/89db55160bb8bbb19464cabf17e465bc.png",
    "GRG": "https://static.debank.com/image/eth_token/logo_url/0x4fbb350052bca5417566f188eb2ebce5b19bc964/0fbf2e6f706029bde207be5e10dc6040.png",
    "EIGEN": "https://static.debank.com/image/project/logo_url/eigenlayer/60961d60d58c7619cf845ff06b2236af.png"
}
PROTOCOL_LOGOS = {
    "Aave V3":           "https://static.debank.com/image/project/logo_url/aave3/54df7839ab09493ba7540ab832590255.png",
    "Aerodrome":         "https://static.debank.com/image/project/logo_url/base_aerodrome/f02d753bc321dc8ba480f0424a686482.png",
    "Curve":             "https://static.debank.com/image/project/logo_url/curve/aa991be165e771cff87ae61e2a61ef68.png",
    "ether.fi":          "https://static.debank.com/image/project/logo_url/etherfi/6c3ea6e8f02322fa9b417e0726978c41.png",
    "Fluid":             "https://static.debank.com/image/project/logo_url/fluid/faeb92cc788df0676eb21db724f80704.png",
    "Curve LlamaLend":   "https://static.debank.com/image/project/logo_url/curve/aa991be165e771cff87ae61e2a61ef68.png",
    "Pendle V2":         "https://static.debank.com/image/project/logo_url/pendle2/d5cfacd3b8f7e0ec161c0de9977cabbd.png",
    "Merkl":             "https://static.debank.com/image/project/logo_url/merkl/7c4a97689b3310cc3436bc6e1a215476.png",
    "Venus":             "https://static.debank.com/image/project/logo_url/bsc_venus/07de2297c7aec45b2d52d9e75754789f.png",
    "Balancer V2":       "https://static.debank.com/image/project/logo_url/balancer2/4318f98916b139a44996fc06531e9074.png",
    "Aerodrome V3":      "https://static.debank.com/image/project/logo_url/base_aerodrome/f02d753bc321dc8ba480f0424a686482.png",
    "Cygnus Finance":    "https://static.debank.com/image/project/logo_url/base_cygnus/e06bf44972afa9a00a230dd537975223.png",
    "Maverick V2":       "https://static.debank.com/image/project/logo_url/maverick/a7a186ccd39807ad670d5ead78effb4e.png",
    "Convex":            "https://static.debank.com/image/project/logo_url/convex/f7de1d568d41ea275b35558c391ab86c.png",
    "LIDO":              "https://static.debank.com/image/project/logo_url/lido/081388ebc44fa042561749bd5338d49e.png",
    "Optimism Bridge":   "https://static.debank.com/image/project/logo_url/optimism_bridge/5b42ae489cd2181dd500137c21ca00e2.png",
    "NURI Exchange":     "https://static.debank.com/image/project/logo_url/scrl_nuri/a8d3cd4269ca15424452a19e7cad8606.png",
    "Silo":              "https://static.debank.com/image/project/logo_url/silo/c1c547e12ac67b71dcf555f4fba77210.png",
    "SushiSwap":         "https://static.debank.com/image/project/logo_url/sushiswap/248a91277aac1ac16a457b8f61957089.png",
    "Yearn V2":          "https://static.debank.com/image/project/logo_url/yearn2/b0f88529a907964dbbf154337906db19.png",
}
COLOR_JSON = {
    "Ethereum":"#627EEA","Arbitrum":"#28A0F0","Base":"#0052FF","Scroll":"#FEDA03","Katana":"#F6FF09","SwellChain":"#2f43ec",
    "Curve":"#FF007A","Aave":"#B6509E","Lido":"#4DB0F2","Aerodrome":"#1AAB9B",
    "ether.fi":"#800DEE","Aave V3":"#B6509E","ETH":"#627EEA","Stables":"#2775ca","Uniswap V3":"#F50DB4",
}
BLOCKCHAIN_LOGOS = {
    "Optimism"  : "https://static.debank.com/image/chain/logo_url/op/68bef0c9f75488f4e302805ef9c8fc84.png",
    "Arbitrum"  : "https://static.debank.com/image/chain/logo_url/arb/854f629937ce94bebeb2cd38fb336de7.png",
    "BNB Chain" : "https://static.debank.com/image/chain/logo_url/bsc/bc73fa84b7fc5337905e527dadcbc854.png",
    "Ethereum"  : "https://static.debank.com/image/chain/logo_url/eth/42ba589cd077e7bdd97db6480b0ff61d.png",
    "Base"      : "https://static.debank.com/image/chain/logo_url/base/ccc1513e4f390542c4fb2f4b88ce9579.png",
    "Berachain" : "https://static.debank.com/image/chain/logo_url/bera/89db55160bb8bbb19464cabf17e465bc.png",
    "Celo"      : "https://static.debank.com/image/chain/logo_url/celo/faae2c36714d55db1d7a36aba5868f6a.png",
    "MobileCoin": "https://static.debank.com/image/chain/logo_url/mobm/fcfe3dee0e55171580545cf4d4940257.png",   # mobm
    "Scroll"    : "https://static.debank.com/image/chain/logo_url/scrl/1fa5c7e0bfd353ed0a97c1476c9c42d2.png",
    "Gnosis"    : "https://static.debank.com/image/chain/logo_url/xdai/43c1e09e93e68c9f0f3b132976394529.png",   # xdai
    "Blast"     : "https://static.debank.com/image/chain/logo_url/blast/15132294afd38ce980639a381ee30149.png",
    "Linea"     : "https://static.debank.com/image/chain/logo_url/linea/32d4ff2cf92c766ace975559c232179c.png",
    "Polygon"   : "https://static.debank.com/image/chain/logo_url/matic/52ca152c08831e4765506c9bd75767e8.png",
    "zkSync Era": "https://static.debank.com/image/chain/logo_url/era/2cfcd0c8436b05d811b03935f6c1d7da.png",
    "Fantom"    : "https://static.debank.com/image/chain/logo_url/ftm/14133435f89637157a4405e954e1b1b2.png",
    "Proof-of-Play Zealy": "https://static.debank.com/image/chain/logo_url/pze/a2276dce2d6a200c6148fb975f0eadd3.png",  # pze
    "Avalanche" : "https://static.debank.com/image/chain/logo_url/avax/4d1649e8a0c7dec9de3491b81807d402.png",
    "Corn"      : "https://static.debank.com/image/chain/logo_url/corn/2ac7405fee5fdeee5964ba0bcf2216f4.png",
    "Klaytn"    : "https://static.debank.com/image/chain/logo_url/klay/4182ee077031d843a57e42746c30c072.png",
    "Mode"      : "https://static.debank.com/image/chain/logo_url/mode/466e6e12f4fd827f8f497cceb0601a5e.png",
    "Katana"    : "https://static.debank.com/image/chain/logo_url/katana/0202d6aecd963a9c0b2afb56c4d731b5.png",
    "Plasma"    : "https://static.debank.com/image/chain/logo_url/plasma/baafefce3b9d43b12b0c016f30aff140.png",
    "SwellChain": "https://static.debank.com/image/chain/logo_url/swell/3e98b1f206af5f2c0c2cc4d271ee1070.png",
}

# ──────── CSS for fixed-width tables ────────
st.markdown("""
<style>
table {table-layout:fixed;width:100%}

/* keep long text from overflowing */
th,td {overflow:hidden;text-overflow:ellipsis;white-space:nowrap}

/* new column widths – order: Wallet | Chain | Token | Token Balance | USD Value */
th:nth-child(1), td:nth-child(1) {width:15%}   /* Wallet  */
th:nth-child(2), td:nth-child(2) {width:15%}   /* Chain   */
th:nth-child(3), td:nth-child(3) {width:35%}   /* Token   */
th:nth-child(4), td:nth-child(4) {width:25%}   /* Token Balance */
th:nth-child(5), td:nth-child(5) {width:10%}   /* USD Value */

</style>""", unsafe_allow_html=True)

# ───────────── Google-Sheets helpers ─────────────
def _gc():
    creds = Credentials.from_service_account_info(
        SA_INFO, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")   # exactly 42-char EVM address

@st.cache_data(ttl=600, show_spinner=False)
def load_wallets() -> list[str]:
    """
    Read the “addresses” worksheet (col A) and return only well-formed
    0x…40-hex-char addresses.

    ── Behaviour ─────────────────────────────────────────────
      • On Google-Sheets error → warn once and return [].
      • On malformed rows     → warn with a short preview; bad rows are skipped.
      • If no valid rows left → warn once and return [] (dashboard will still
                                run, there’ll just be no on-chain data).
    """
    # 1) pull the raw column values
    try:
        ws   = _gc().open_by_key(SHEET_ID).worksheet("liquid_vaults")
        raw  = [v.strip() for v in ws.col_values(1)]
    except Exception as e:
        st.warning(f"⚠️ Unable to read the *addresses* sheet – {e}")
        return []

    # 2) separate good vs. bad rows
    good = [a for a in raw if ADDR_RE.fullmatch(a)]
    bad  = [a for a in raw if a and a.startswith("0x") and not ADDR_RE.fullmatch(a)]

    if bad:
        preview = ", ".join(bad[:3]) + ("…" if len(bad) > 3 else "")
        st.warning(f"⚠️ Ignored {len(bad)} malformed address(es): {preview}")

    if not good:
        st.warning("⚠️ No valid wallet addresses found in the sheet.")
    return good

WALLETS = load_wallets()

# ───────────── Dune helpers ─────────────
@lru_cache(maxsize=1)                      # cache for this run
def dune_prices() -> dict:
    """Return {token_symbol: usd_price} from the Dune query."""
    api  = st.secrets["DUNE_API_KEY"]
    qid  = st.secrets["DUNE_QUERY_ID"]
    url  = f"https://api.dune.com/api/v1/query/{qid}/results?api_key={api}"
    try:
        resp = requests.get(url, timeout=15).json()
        rows = resp["result"]["rows"]      # [{'token_symbol': 'weETH', 'usd_price': 3300}, …]
        return {r["token_symbol"]: float(r["usd_price"]) for r in rows}
    except Exception as e:
        st.warning(f"⚠️ Dune price fetch failed ({e}) – off-chain balances skipped.")
        return {}

# ───────────── helpers ─────────────
def first_symbol(t): return t.get("optimized_symbol") or t.get("display_symbol") or t.get("symbol")
def link_wallet(a):  return f"[{a[:6]}…{a[-4:]}](https://debank.com/profile/{a})"
def fmt_usd(v: float) -> str:
    sign = "-" if v < 0 else ""          
    v = abs(v)
    if v >= 1e6:
        return f"{sign}${v/1e6:.2f}M"
    if v >= 1e3:
        return f"{sign}${v/1e3:.1f}K"
    return     f"{sign}${v:,.0f}"

# ───────────── NEW: token-category lookup ─────────────
@st.cache_data(ttl=600, show_spinner=False)
def load_token_categories() -> dict[str, str]:
    """
    Read the *token_category* sheet (col A = keyword, col B = category)
    and return a mapping { keyword_lower : CategoryName }.
    """
    try:
        ws   = _gc().open_by_key(SHEET_ID).worksheet("token_category")
        rows = [tuple(map(str.strip, r[:2]))
                for r in ws.get_all_values() if r and r[0].strip()]
        return {k.lower(): v for k, v in rows if v}
    except Exception as e:
        st.warning(f"⚠️ Unable to read *token_category* sheet – {e}")
        return {}

TOKEN_CATS = load_token_categories()

def token_category(tok: str) -> str:
    """
    Map a token symbol → Category using the sheet-driven rules above.
    Falls back to 'Others'.
    """
    symbol = tok.lower()
    for kw, cat in TOKEN_CATS.items():          # first match wins
        if kw in symbol:
            return cat
    return "Others"

def md_table(df,cols):
    hdr="| "+" | ".join(cols)+" |"; sep="| "+" | ".join("---" for _ in cols)+" |"
    rows=["| "+" | ".join(str(r[c]) for c in cols)+" |" for _,r in df.iterrows()]
    return "\n".join([hdr,sep,*rows])
# Simple retry helper – sleeps & retries on HTTP 429 / 5xx
def _safe_get(url: str, params: dict, headers: dict, retries: int = 3):
    for attempt in range(retries):
        r = requests.get(url, params=params, headers=headers, timeout=15)
        if r.status_code < 429 or attempt == retries - 1:
            # success (2xx) or non-retryable / out-of-retries
            return r
        # hit rate-limit or temporary error → back-off & retry
        sleep_for = 0.25 * (2 ** attempt)          # 0.25s, 0.5s, 1s, …
        time.sleep(sleep_for)
    return r   # last response (let caller decide what to do)
def ensure_utc(ts: pd.Timestamp):
    return ts if ts.tzinfo else ts.tz_localize("UTC")
@st.cache_data(ttl=600, show_spinner=False)
def load_wallet_snapshot(day: datetime.date) -> pd.DataFrame:
    try:
        ws = _gc().open_by_key(SHEET_ID).worksheet(WALLET_SHEET)
        df = pd.DataFrame(ws.get_all_records())
        if df.empty:
            return df
        df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y",
                                    errors="coerce").dt.date
        df = df[df["date"] == day]
        df = df.rename(columns={
            "full_address": "Wallet",
            "blockchain":   "Chain",
            "token_symbol": "Token",
            "token_balance": "Token Balance",
            "usd_value":    "USD Value",
        })
        df["USD Value"] = pd.to_numeric(df["USD Value"], errors="coerce")
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        last_ts = df["timestamp"].max()
        df = df[df["timestamp"] == last_ts].copy()
        df = (df.sort_values("timestamp", ascending=False)
                .groupby(["Wallet", "Chain", "Token"], as_index=False)   
                .first())

        return df
    except Exception:
        return pd.DataFrame(columns=[
            "Wallet", "Chain", "Token",
            "Token Balance", "USD Value", "date"
        ])

# ───────── Debank ONE-CALL helpers ──────────
@st.cache_data(ttl=600, show_spinner=False)
def debank_all_tokens(wallet: str) -> list[dict]:
    url = "https://pro-openapi.debank.com/v1/user/all_token_list"
    r   = _safe_get(
            url,
            {"id": wallet,
             "chain_ids": ",".join(CHAIN_IDS),   
             "is_all": False},                   
            headers,
          )

    if r.status_code != 200:
        st.warning(
            f"Debank {wallet[:6]}…{wallet[-4:]} all_token_list: "
            f"{r.status_code} – {r.text[:100]}"
        )
        return []

    rows = []
    for t in r.json():                           
        price, amt = t.get("price", 0), t.get("amount", 0)
        if price <= 0:
            continue

        chain_id = t.get("chain") or t.get("chain_id")
        rows.append({
            "Wallet":        wallet,
            "Chain":         CHAIN_NAMES.get(chain_id, chain_id),
            "Token":         first_symbol(t),
            "Token Balance": amt,
            "USD Value":     amt * price,
        })
    return rows


@st.cache_data(ttl=600, show_spinner=False)
def debank_all_protocols(wallet: str) -> list[dict]:
    url = "https://pro-openapi.debank.com/v1/user/all_complex_protocol_list"
    r   = _safe_get(
            url,
            {"id": wallet, "chain_ids": ",".join(CHAIN_IDS)},
            headers,
          )

    if r.status_code != 200:
        st.warning(
            f"Debank {wallet[:6]}…{wallet[-4:]} complex_protocol_list: "
            f"{r.status_code} – {r.text[:100]}"
        )
        return []

    return r.json()


# ───────────── off-chain sheet fetcher ─────────────
@st.cache_data(ttl=600, show_spinner=False)
def fetch_offchain() -> pd.DataFrame:
    """
    Sheet “offchain” has:
      wallet_address | blockchain | token_symbol | token_balance | protocol
    Convert it to the same shape as df_protocols.
    """
    try:
        ws  = _gc().open_by_key(SHEET_ID).worksheet("liquid_vaults_offchain")
        df  = pd.DataFrame(ws.get_all_records())
        if df.empty:
            return pd.DataFrame(columns=df_protocols.columns)  # placeholder
        prices = dune_prices()           # live prices from Dune
        df["usd_price"]   = df["token_symbol"].map(prices)
        df["USD Value"]   = pd.to_numeric(df["token_balance"], errors="coerce") * df["usd_price"]
        df = df.dropna(subset=["USD Value"])                   # drop if no price
        df = df.rename(columns={
            "wallet_address": "Wallet",
            "blockchain":     "Blockchain",
            "token_symbol":   "Token",
            "token_balance":  "Token Balance",
            "protocol":       "Protocol",
        })
        df["Classification"] = ""         # leave empty
        df["Pool"]           = ""         # N/A
        return df[
            ["Protocol", "Classification", "Blockchain", "Pool",
             "Wallet", "Token", "Token Balance", "USD Value"]
        ]
    except Exception as e:
        st.warning(f"⚠️ Off-chain sheet fetch failed ({e}) – skipping.")
        return pd.DataFrame(columns=df_protocols.columns)

# ───────────── sidebar ─────────────
sel_wallets = WALLETS
sel_chains  = st.sidebar.multiselect("Chains",  list(CHAIN_NAMES.values()), default=list(CHAIN_NAMES.values()))

# ───────────── build dfs ─────────────
wallet_rows = []
for w in sel_wallets:
    wallet_rows += debank_all_tokens(w)

cols_wallet = ["Wallet", "Chain", "Token", "Token Balance", "USD Value"]
df_wallets  = pd.DataFrame(wallet_rows, columns=cols_wallet)

df_wallets = df_wallets[df_wallets["Chain"].isin(sel_chains)].copy()
df_wallets["USD Value"] = pd.to_numeric(df_wallets["USD Value"], errors="coerce")
df_wallets = df_wallets[df_wallets["USD Value"] >= 1]

# If no rows were returned, warn once
if "Chain" in df_wallets.columns:
    df_wallets = df_wallets[df_wallets["Chain"].isin(sel_chains)].copy()
else:
    st.warning("⚠️ No token data returned from Debank – frame is empty.")
    df_wallets = pd.DataFrame(columns=["Wallet","Chain","Token",
                                       "Token Balance","USD Value"])

# Apply chain filter & basic housekeeping
df_wallets["USD Value"] = pd.to_numeric(df_wallets["USD Value"], errors="coerce")
df_wallets = df_wallets[df_wallets["USD Value"] >= 1]      # drop rows < $1                    # filter <1

prot_rows = []
for w in sel_wallets:
    for p in debank_all_protocols(w):
        for it in p.get("portfolio_item_list", []):
            desc = (it.get("detail") or {}).get("description") or ""
            detail = it.get("detail") or {}
            toks   = (
                detail.get("supply_token_list", []) +      # ← supply
                detail.get("reward_token_list", []) +      # ← rewards
                [
                    {**bt, "amount": -bt.get("amount", 0)} # ← borrow  ➜ negate amount
                    for bt in detail.get("borrow_token_list", [])
                ]
            )
            for t in toks:
                price, amt = t.get("price", 0), t.get("amount", 0)
                if price <= 0:
                    continue
                sym = desc if desc and not desc.startswith("#") else first_symbol(t)
                prot_rows.append({
                    "Protocol":      p.get("name"),
                    "Classification": it.get("name", ""),
                    "Blockchain":    CHAIN_NAMES.get(p.get("chain"), p.get("chain")),
                    "Pool":          it.get("pool", {}).get("id", ""),
                    "Wallet":        w,
                    "Token":         sym,
                    "Token Balance": amt,
                    "USD Value":     amt * price,
                })
cols_proto = ["Protocol", "Classification", "Blockchain", "Pool",
              "Wallet", "Token", "Token Balance", "USD Value"]
df_protocols = pd.DataFrame(prot_rows, columns=cols_proto) 
if "Blockchain" in df_protocols.columns:
    df_protocols = df_protocols[df_protocols["Blockchain"].isin(sel_chains)].copy()
else:
    st.warning("⚠️ No protocol data returned from Debank – frame is empty.")
    df_protocols = pd.DataFrame(columns=cols_proto)
df_protocols["USD Value"]=pd.to_numeric(df_protocols["USD Value"],errors="coerce")
df_protocols = df_protocols[abs(df_protocols["USD Value"]) >= 1]
# fetch + append off-chain balances
df_offchain   = fetch_offchain()
df_protocols  = pd.concat([df_protocols, df_offchain], ignore_index=True)

# ───────────── snapshot (unchanged) ─────────────
def write_snapshot():
    if df_protocols.empty and df_wallets.empty: return
    hour=datetime.datetime.utcnow().replace(minute=0,second=0,microsecond=0).isoformat()
    gc=_gc(); sh=gc.open_by_key(SHEET_ID)
    try: ws=sh.worksheet("liquid_vaults_history")
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
    rows.append([hour, "protocol", "Wallet Balances",
             round(df_wallets["USD Value"].sum(), 2)])
    # ─── snapshot wallet balances ───────────────────────────────
    try:
        wb_ws = sh.worksheet(WALLET_SHEET)
    except gspread.WorksheetNotFound:
        wb_ws = sh.add_worksheet(WALLET_SHEET, rows=2, cols=6)
        wb_ws.append_row(
            ["full_address", "blockchain", "token_symbol",
             "token_balance", "usd_value", "date", "timestamp"]
        )

    timestamp_iso = datetime.datetime.utcnow().isoformat(timespec="seconds")
    date_str      = datetime.datetime.utcnow().strftime("%d-%m-%Y")
    
    wb_rows = (
        df_wallets.assign(date=date_str, timestamp=timestamp_iso)   # ⬅️ add both cols
                  .rename(columns={
                      "Wallet":        "full_address",
                      "Chain":         "blockchain",
                      "Token":         "token_symbol",
                      "Token Balance": "token_balance",
                      "USD Value":     "usd_value",
                  })
                  [["full_address", "blockchain", "token_symbol",
                    "token_balance", "usd_value", "date", "timestamp"]]
                  .values.tolist()
    )
    wb_ws.append_rows(wb_rows, value_input_option="RAW")
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
st.markdown("## 🔍 Vault Positions Breakdown")
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
    proto_df.columns = ["protocol", "usd"]       
    present = proto_df["protocol"].tolist()
    colour_map = {p: COLOR_JSON[p]          
                    for p in present
                    if p in COLOR_JSON}
    fallback_cycle = itertools.cycle(px.colors.qualitative.Plotly)
    for p in present:
        if p not in colour_map:
            colour_map[p] = next(fallback_cycle)
    fig_proto = px.pie(
        proto_df,
        names="protocol",
        values="usd",
        color="protocol",
        hole=.4,
        color_discrete_map=colour_map,
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
        ws=_gc().open_by_key(SHEET_ID).worksheet("liquid_vaults_history")
        h=pd.DataFrame(ws.get_all_records())
        h["usd_value"]=pd.to_numeric(h["usd_value"],errors="coerce")
        h["timestamp"]=pd.to_datetime(h["timestamp"],utc=True,errors="coerce")
        return h.dropna(subset=["timestamp","usd_value"])
    except: return pd.DataFrame(columns=["timestamp","history_type","name","usd_value"])

hist = load_history()

# ── NEW: keep only the *latest* snapshot of every day ──────────────
if not hist.empty:
    hist_day = (hist.sort_values("timestamp")                         # oldest→newest
                    .assign(day=lambda d: d["timestamp"].dt.normalize())
                    .groupby(["history_type", "name", "day"], as_index=False)
                    .last())                                          # row with latest hour
else:
    hist_day = hist

st.markdown("## 📈 Historical Data")
area1,area2=st.columns(2)

if not hist.empty:
    # protocol area
    p = (hist_day[hist_day["history_type"] == "protocol"]
        .sort_values("day")              
        .copy())
    if not p.empty:
        p["usd_value"]=pd.to_numeric(p["usd_value"],errors="coerce").fillna(0)
        top=p.groupby("name")["usd_value"].last().nlargest(10).index
        p.loc[~p["name"].isin(top),"name"]="Others"
        p = (p.groupby(["day", "name"], as_index=False)    
               .agg({"usd_value": "sum"})) 
        fig_p = px.area(
            p, x="day", y="usd_value", color="name",
            color_discrete_map=COLOR_JSON                          
        )
        fig_p.update_layout(title="Top Protocols",xaxis_title="Date",yaxis_title="")
        fig_p.update_yaxes(tickformat="$~s")
        fig_p.update_xaxes(tickformat="%b %d %Y")
        area1.plotly_chart(fig_p,use_container_width=True)

    # token area
    t = (hist_day[hist_day["history_type"] == "token"]
        .sort_values("day")             
        .copy())
    if not t.empty:
        t["usd_value"]=pd.to_numeric(t["usd_value"],errors="coerce").fillna(0)
        cats=["ETH","Stables","Others"]
        t.loc[~t["name"].isin(cats),"name"]="Others"
        fig_t = px.area(
            t, x="day", y="usd_value", color="name",
            color_discrete_map=COLOR_JSON                          
        )
        fig_t.update_layout(title="Token Categories",xaxis_title="Date",yaxis_title="")
        fig_t.update_yaxes(tickformat="$~s")
        fig_t.update_xaxes(tickformat="%b %d %Y")
        area2.plotly_chart(fig_t,use_container_width=True)

st.markdown("---")

# ───────────── wallet table ─────────────
# --- wallet-table filters -----------------------------------
col_w, col_t, col_d = st.columns(3)

with col_w:
    wallet_input = st.text_input(
        "👛 Wallet filter",
        key="wal_filter",
        placeholder="All Wallets",
        help="Multiple Addresses are separated by commas, e.g. 0x345…5775, 0x4646…5656"
    )

with col_t:
    token_input = st.text_input(
        "🪙 Token filter",
        key="tok_filter",
        placeholder="All Tokens",
        help="Multiple Tokens are separated by commas, e.g. weETH, WETH"
    )

with col_d:
    snap_date = st.date_input(
        "📅 Snapshot date",
        datetime.date.today(),
        help="Pick a past date to load the latest snapshot for that day. Today = live data."
    )

filter_wallets = [w.strip().lower() for w in wallet_input.split(",") if w.strip()]
filter_tokens = [t.strip().upper() for t in token_input.split(",") if t.strip()]

# dataframe view after applying the two filters
df_wallets_view = df_wallets.copy()
if filter_wallets:
    df_wallets_view = df_wallets_view[df_wallets_view["Wallet"].str.lower().isin(filter_wallets)]
if filter_tokens:
    df_wallets_view = df_wallets_view[df_wallets_view["Token"].str.upper().isin(filter_tokens)]

st.subheader("💰 Wallet Balances")
if not df_wallets_view.empty:
    live_df   = df_wallets.copy()
    hist_df   = load_wallet_snapshot(snap_date) if snap_date != datetime.date.today() else pd.DataFrame()
    if hist_df.empty and snap_date != datetime.date.today():
        st.warning("No snapshot found for that date – showing live data instead.")
    src_df    = hist_df if not hist_df.empty else live_df

    df_filtered = src_df.copy()
    if filter_wallets:
        df_filtered = df_filtered[df_filtered["Wallet"].str.lower().isin(filter_wallets)]
    if filter_tokens:
        df_filtered = df_filtered[df_filtered["Token"].str.upper().isin(filter_tokens)]

    if df_filtered.empty:
        st.info("No wallet balances match the current filters.")
        st.markdown("---")
    
    else:
        df = df_filtered.copy()
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df = (df.sort_values(["Wallet", "Chain", "Token", "timestamp"],
                                 ascending=[True,   True,    True,     False])
                    .drop_duplicates(subset=["Wallet", "Chain", "Token"], keep="first"))

            
        df = df.sort_values("USD Value", ascending=False)

        csv_df = df.rename(
            columns={
                "Wallet":        "full_address",
                "Chain":         "blockchain",
                "Token":         "token_symbol",
                "Token Balance": "token_balance",
                "USD Value":     "usd_value",
            }
        )
        csv_df["date"] = snap_date.strftime("%d-%m-%Y")
        df["USD Value"] = df["USD Value"].apply(fmt_usd)
        df["Token Balance"] = df["Token Balance"].apply(lambda x: f"{x:,.4f}")
        df["Wallet"] = df["Wallet"].apply(link_wallet)
        df["Token"]  = df.apply(
            lambda r: f'<img src="{TOKEN_LOGOS.get(r.Token) or BLOCKCHAIN_LOGOS.get(r.Chain,"")}" '
                      f'width="16" style="vertical-align:middle;margin-right:4px;"> {r.Token}',
            axis=1
        )
        if "timestamp" in df.columns:
            df = df.drop(columns=["timestamp"])
    
        st.markdown(md_table(df,["Wallet","Chain","Token","Token Balance","USD Value"]),
                    unsafe_allow_html=True)
    
        csv_bytes = csv_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download CSV", csv_bytes,
                           file_name="liquid_vaults_wallet_balances.csv",
                           mime="text/csv")

else:
    st.info("No wallet balances match the current filters.")

st.markdown("---")   # separator before protocol section

# ───────────── protocol positions table ─────────────
st.subheader("🏦 DeFi Protocol Positions")
if not df_protocols.empty:
    dfp=df_protocols.copy()
    dfp_raw = df_protocols.copy()
    dfp["USD Value"]=dfp["USD Value"].apply(fmt_usd)
    dfp["Token Balance"]=dfp["Token Balance"].apply(lambda x:f"{x:,.4f}")
    dfp["Wallet"]=dfp["Wallet"].apply(link_wallet)

    order = (
        dfp_raw                                  # <- raw (numeric) frame
        .groupby("Protocol")["USD Value"]
        .sum()
        .sort_values(ascending=False)
    )

    for proto in order.index:
        st.markdown(
            f'<h3><img src="{PROTOCOL_LOGOS.get(proto,"")}" width="24" style="vertical-align:middle;margin-right:6px;">'
            f'{proto} ({fmt_usd(order[proto])})</h3>', unsafe_allow_html=True)

        sub = dfp[dfp["Protocol"] == proto].copy()

                # --- order classifications (sub-categories) by total USD value ---
        cls_order = (
            dfp_raw[dfp_raw["Protocol"] == proto]
            .groupby("Classification")["USD Value"]
            .sum()
            .sort_values(ascending=False)
            .index
        )

        for cls in cls_order:
            if pd.isna(cls):            # skip empty classifications
                continue
            st.markdown(f"<h4 style='margin:6px 0 2px'>{cls}</h4>", unsafe_allow_html=True)

            # ── special handling for Liquidity Pool rows ──
            if cls == "Liquidity Pool" and proto not in ("Pendle", "Pendle V2"):
                raw_lp = dfp_raw[
                    (dfp_raw["Protocol"] == proto) &
                    (dfp_raw["Classification"] == cls)
                ].copy()
                raw_lp.rename(columns={"Blockchain": "Chain"}, inplace=True)

                agg_rows = []
                for pid, grp in raw_lp.groupby("Pool"):

                    # --- collapse duplicate token rows (supply + reward) ---
                    grp = (
                        grp.groupby("Token", as_index=False)
                           .agg({
                                "USD Value":     "sum",
                                "Token Balance": "sum",
                                "Wallet":        "first",
                                "Chain":         "first",
                           })
                    )

                    token_col = " + ".join(
                        f'<img src="{TOKEN_LOGOS.get(t) or BLOCKCHAIN_LOGOS.get(grp["Chain"].iloc[0],"")}" '
                        f'width="16" style="vertical-align:middle;margin-right:4px;"> {t}'
                        for t in grp["Token"]
                    )

                    bal_col = " + ".join(
                        f'{bal:,.4f} {tok}' for tok, bal in
                        zip(grp["Token"], grp["Token Balance"])
                    )

                    agg_rows.append({
                        "Wallet":        link_wallet(grp["Wallet"].iloc[0]),
                        "Chain":         grp["Chain"].iloc[0],
                        "Token":         token_col,
                        "Token Balance": bal_col,
                        "USD Value":     grp["USD Value"].sum(),
                    })


                part = pd.DataFrame(agg_rows).sort_values("USD Value", ascending=False)
                part["USD Value"] = part["USD Value"].apply(fmt_usd)

            # ── all other classifications ──
            else:
                part = dfp_raw[
                    (dfp_raw["Protocol"] == proto) &
                    (dfp_raw["Classification"] == cls)
                ].copy().sort_values("USD Value", ascending=False)

                part.rename(columns={"Blockchain": "Chain"}, inplace=True)
                part["Wallet"] = part["Wallet"].apply(link_wallet)
                part["Token"] = part.apply(
                    lambda r: f'<img src="{TOKEN_LOGOS.get(r.Token) or BLOCKCHAIN_LOGOS.get(r.Chain,"")}" '
                              f'width="16" style="vertical-align:middle;margin-right:4px;"> {r.Token}',
                    axis=1
                )
                part["Token Balance"] = part["Token Balance"].apply(lambda x: f"{x:,.4f}")
                part["USD Value"]      = part["USD Value"].apply(fmt_usd)

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
