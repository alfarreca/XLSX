import io
import time
import requests
import pandas as pd
import streamlit as st

# =========================
# Page + Dark Theme Styling
# =========================
st.set_page_config(page_title="YF Ticker Filler", page_icon="📈", layout="wide")
st.markdown(
    """
<style>
  .stApp { background: #0e1117; color: #e4e6eb; }
  .stMarkdown, .stTextInput, .stSelectbox, .stFileUploader, .stDataFrame { color: #e4e6eb !important; }
  .st-bk { background: #0e1117 !important; }
  .stButton>button, .stDownloadButton>button {
      background-color: #1b1f2a !important; color: #e4e6eb !important; border: 1px solid #2a2f3a;
  }
  .stButton>button:hover, .stDownloadButton>button:hover {
      background-color: #2a2f3a !important;
  }
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# Constants & HTTP helpers
# =========================
YAHOO_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

def yahoo_search(query: str, lang="en-US", quotes_count=5, news_count=0, region="US", timeout=8):
    """
    Call Yahoo Finance public search endpoint (no API key).
    Returns list of quote dicts (symbol, shortname, longname, exchDisp, quoteType, ...).
    """
    params = {
        "q": query,
        "lang": lang,
        "region": region,
        "quotesCount": quotes_count,
        "newsCount": news_count,
    }
    headers = {"User-Agent": DEFAULT_UA}
    try:
        r = requests.get(YAHOO_SEARCH_URL, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        return data.get("quotes", []) or []
    except Exception:
        return []

def pick_best_match(
    quotes,
    prefer_exchanges=None,
    prefer_regions=None,
    allow_types=("EQUITY", "ETF", "FUND", "CURRENCY", "INDEX"),
):
    """
    Choose the 'best' match using heuristics:
      - Prefer allowed quoteType
      - Nudge if exchange/region matches user preference
      - Prefer symbols that are not indices (^)
      - Prefer EQUITY slight bias
    """
    if not quotes:
        return None

    allow_types = set(x.upper() for x in (allow_types or []))
    filtered = [q for q in quotes if str(q.get("quoteType", "")).upper() in allow_types] or quotes[:]

    pe = set(x.upper() for x in (prefer_exchanges or []))
    pr = set(x.upper() for x in (prefer_regions or []))

    def score(q):
        s = 0.0
        exch_disp = (q.get("exchDisp") or "").upper()
        exch = (q.get("exchange") or "").upper()
        symbol = str(q.get("symbol", "") or "")
        qtype = str(q.get("quoteType", "") or "").upper()

        # Exchanges / regions nudges
        if pe and (exch_disp in pe or exch in pe):
            s += 5
        if pr and any(p in (exch_disp + " " + exch) for p in pr):
            s += 3

        # Prefer regular symbols (avoid ^ indices)
        if symbol and not symbol.startswith("^"):
            s += 1

        # Prefer equities slightly
        if qtype == "EQUITY":
            s += 1

        # Prefer richer names present
        if q.get("longname") or q.get("shortname"):
            s += 0.5

        # Small nudge for having exchange label
        if exch_disp:
            s += 0.25

        return s

    ranked = sorted(filtered, key=score, reverse=True)
    return ranked[0] if ranked else None

def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return buf.getvalue()

# =========================
# UI
# =========================
st.title("📈 Auto-Fill Yahoo Finance Tickers from Excel")
st.caption("Upload an Excel with a **Name** column (or choose the column). I’ll find best-match Yahoo Finance symbols. Optimized for EU.")

with st.expander("⚙️ Matching Options", expanded=True):
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        prefer_regions = st.multiselect(
            "Prefer Regions (matched via exchange label text)",
            ["US", "EU", "GB", "DE", "FR", "ES", "PT", "IT", "NL", "SE", "CH", "IE", "NO"],
            default=["EU", "GB", "DE", "FR", "ES", "PT", "IT", "NL", "SE", "CH", "IE", "NO"],
        )
    with c2:
        prefer_exchanges = st.multiselect(
            "Prefer Exchanges",
            [
                "EURONEXT", "XETRA", "FRANKFURT", "LSE", "MILAN", "MADRID", "SIX SWISS",
                "NASDAQ", "NYSE", "AMEX", "OSLO", "STOCKHOLM", "HELSINKI", "COPENHAGEN",
                "BRUSSELS", "LISBON", "DUBLIN",
            ],
            default=["EURONEXT", "XETRA", "FRANKFURT", "LSE", "MILAN", "MADRID", "SIX SWISS", "LISBON", "BRUSSELS", "DUBLIN"],
        )
    with c3:
        allow_types = st.multiselect(
            "Allow Quote Types",
            ["EQUITY", "ETF", "FUND", "CURRENCY", "INDEX"],
            default=["EQUITY", "ETF", "FUND"],
        )

uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])

if uploaded:
    # Read
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"Could not read the Excel file: {e}")
        st.stop()

    if df.empty:
        st.warning("The uploaded Excel is empty.")
        st.stop()

    st.subheader("Preview")
    st.dataframe(df.head(20), use_container_width=True)

    # Column mapping
    cols = list(df.columns)
    st.markdown("### Column Mapping")
    col1, col2 = st.columns(2)
    with col1:
        default_name_idx = cols.index("Name") if "Name" in cols else 0
        name_col = st.selectbox("Column with **Company Name**", options=cols, index=default_name_idx)
    with col2:
        symbol_col_existing = st.selectbox("Existing Symbol column (optional)", options=["<none>"] + cols, index=0)

    # Behavior toggles
    only_fill_blank = st.checkbox("Only fill rows where Symbol is blank/missing", value=True)
    rate_limit_ms = st.slider("Per-row delay (ms) to be gentle with Yahoo", min_value=0, max_value=1000, value=150, step=50)

    st.markdown("---")
    run = st.button("🔎 Fetch Best-Match Tickers")

    if run:
        if not name_col:
            st.error("Please select the company name column.")
            st.stop()

        out_df = df.copy()

        # Prepare Symbol column to write into
        if symbol_col_existing != "<none>" and symbol_col_existing in out_df.columns:
            symbol_col = symbol_col_existing
        else:
            symbol_col = "Symbol"
            if "Symbol" not in out_df.columns:
                out_df["Symbol"] = ""

        if only_fill_blank:
            needs_fill_mask = out_df[symbol_col].isna() | (out_df[symbol_col].astype(str).str.strip() == "")
        else:
            needs_fill_mask = pd.Series([True] * len(out_df), index=out_df.index)

        total = int(needs_fill_mask.sum())
        if total == 0:
            st.info("Nothing to fill. All rows already have symbols (or filter left no rows).")
            # Still allow download of current out_df
            st.download_button(
                "📥 Download Excel (unchanged)",
                data=to_excel_bytes(out_df),
                file_name="with_yf_symbols.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.stop()

        results = []
        progress = st.progress(0)
        status = st.empty()

        pe = [x.upper() for x in prefer_exchanges] if prefer_exchanges else None
        pr = [x.upper() for x in prefer_regions] if prefer_regions else None
        at = [x.upper() for x in allow_types] if allow_types else None

        for i, (idx, row) in enumerate(out_df[needs_fill_mask].iterrows(), start=1):
            company = safe_str(row[name_col])
            progress.progress(min(i / max(total, 1), 1.0))
            status.write(f"Searching: {company if company else '(blank)'}")

            if not company:
                # No query → no match
                results.append(
                    (idx, "", "", "", "", "", 0.0)
                )
                continue

            quotes = yahoo_search(company, lang="en-US", quotes_count=6, news_count=0, region="US")
            match = pick_best_match(quotes, prefer_exchanges=pe, prefer_regions=pr, allow_types=at)

            if match:
                symbol = match.get("symbol", "") or ""
                shortname = match.get("shortname", "") or ""
                longname = match.get("longname", "") or ""
                exch_disp = match.get("exchDisp", "") or match.get("exchange", "") or ""
                qtype = match.get("quoteType", "") or ""
                conf = 0.85 if (qtype or "").upper() == "EQUITY" else 0.75
            else:
                symbol, shortname, longname, exch_disp, qtype, conf = "", "", "", "", "", 0.0

            out_df.at[idx, symbol_col] = symbol
            results.append((idx, symbol, shortname, longname, exch_disp, qtype, conf))

            # Be gentle
            if rate_limit_ms:
                time.sleep(rate_limit_ms / 1000)

        status.empty()
        progress.empty()

        # Build review table (FIX: use index-aligned join, not merge on dropped key)
        res_df = pd.DataFrame(
            results,
            columns=["_row_idx", "Symbol", "YF_ShortName", "YF_LongName", "Exchange", "QuoteType", "Confidence"],
        )
        review_df = out_df.join(res_df.set_index("_row_idx"), how="left")

        st.success("Done! Review proposed matches below.")
        st.dataframe(review_df, use_container_width=True)

        # Download updated Excel (just the filled symbols)
        st.download_button(
            label="📥 Download Excel with Symbols",
            data=to_excel_bytes(out_df),
            file_name="with_yf_symbols.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        with st.expander("🔍 Transparency: Last 50 matches"):
            st.dataframe(review_df.tail(50), use_container_width=True)

else:
    st.info("Upload an Excel file to begin. Example columns: **Name** (required or selectable), optional **Symbol** to fill-in.")

st.markdown("---")
st.caption("Made for Streamlit Cloud • Fills Yahoo Finance tickers from your uploaded list • Dark UI")
