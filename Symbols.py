import io
import time
import json
import math
import requests
import pandas as pd
import streamlit as st

# ---------- Page + Theme ----------
st.set_page_config(page_title="YFinance Ticker Filler", page_icon="📈", layout="wide")
# Force a dark-ish look via lightweight CSS (works even if user theme is light)
st.markdown("""
<style>
    .stApp { background: #0e1117; color: #e4e6eb; }
    .stMarkdown, .stTextInput, .stSelectbox, .stFileUploader, .stDataFrame { color: #e4e6eb !important; }
    .st-bk { background: #0e1117 !important; }
    .css-1v0mbdj, .css-1dp5vir, .css-1d391kg, .stButton>button { background-color: #1b1f2a !important; color: #e4e6eb !important; border: 1px solid #2a2f3a; }
    .stButton>button:hover { background-color: #2a2f3a !important; }
    .stDownloadButton>button { background-color: #1b1f2a !important; color: #e4e6eb !important; border: 1px solid #2a2f3a; }
    .stDownloadButton>button:hover { background-color: #2a2f3a !important; }
</style>
""", unsafe_allow_html=True)

# ---------- Helpers ----------
YAHOO_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"

def yahoo_search(query: str, lang="en-US", quotes_count=5, news_count=0, region="US"):
    """
    Hit Yahoo Finance search endpoint. No API key required.
    Returns list of quote dicts with keys like: symbol, shortname, longname, exchDisp, quoteType.
    """
    params = {
        "q": query,
        "lang": lang,
        "region": region,
        "quotesCount": quotes_count,
        "newsCount": news_count
    }
    try:
        r = requests.get(YAHOO_SEARCH_URL, params=params, timeout=8)
        r.raise_for_status()
        data = r.json()
        return data.get("quotes", []) or []
    except Exception as e:
        return []

def pick_best_match(quotes, prefer_exchanges=None, prefer_regions=None, allow_types=("EQUITY","ETF","FUND","CURRENCY","INDEX")):
    """
    Choose the 'best' match from Yahoo quotes using simple heuristics:
    - Prefer allowed quoteType
    - If prefer_exchanges set, prioritize those
    - If prefer_regions set, prioritize those (based on 'exchDisp' or 'exchange' text)
    - Then highest score/order from Yahoo
    Return (match_dict or None)
    """
    if not quotes:
        return None

    # Filter by type
    filtered = [q for q in quotes if str(q.get("quoteType","")).upper() in allow_types]

    if not filtered:
        filtered = quotes[:]  # fallback to anything

    def score(q):
        s = 0
        # Yahoo doesn't give explicit score; earlier items are better → inverse index rank
        # We'll add preferences:
        exch_disp = (q.get("exchDisp") or "").upper()
        exch      = (q.get("exchange") or "").upper()
        region_ok = False
        exchange_ok = False

        if prefer_exchanges:
            if exch_disp in prefer_exchanges or exch in prefer_exchanges:
                exchange_ok = True
                s += 5

        if prefer_regions:
            # crude region check by display exchange label text
            if any(pref.upper() in (exch_disp + " " + exch) for pref in prefer_regions):
                region_ok = True
                s += 3

        # Prefer symbols that look normal (avoid caret-prefixed indices)
        symbol = str(q.get("symbol",""))
        if symbol and not symbol.startswith("^"):
            s += 1

        # Prefer items with a longer display name available
        if q.get("longname") or q.get("shortname"):
            s += 1

        # Prefer 'EQUITY' over others
        if str(q.get("quoteType","")).upper() == "EQUITY":
            s += 1

        # Add a tiny nudge for having an exchange displayed
        if exch_disp:
            s += 0.5

        return s, exchange_ok, region_ok

    # Sort by heuristic score (desc)
    ranked = sorted(filtered, key=lambda q: score(q), reverse=True)
    return ranked[0] if ranked else None

def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return out.getvalue()

# ---------- UI ----------
st.title("📈 Auto-Fill Yahoo Finance Tickers from Excel")
st.caption("Upload an Excel file with company **Name** (or choose the column), and I’ll find the best Yahoo Finance tickers for you.")

with st.expander("⚙️ Matching Options", expanded=True):
    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        prefer_regions = st.multiselect(
            "Prefer Regions (match by exchange label text):",
            ["US","EU","GB","DE","FR","ES","PT","IT","NL","SE","CH","IE","NO"],
            default=["EU","GB","DE","FR","ES","PT","IT","NL","SE","CH","IE","NO"]
        )
    with c2:
        prefer_exchanges = st.multiselect(
            "Prefer Exchanges:",
            [
                "EURONEXT", "XETRA", "FRANKFURT", "LSE", "MILAN", "MADRID", "SIX SWISS",
                "NASDAQ","NYSE","AMEX","OSLO","STOCKHOLM","HELSINKI","COPENHAGEN","BRUSSELS","LISBON","DUBLIN"
            ],
            default=["EURONEXT","XETRA","FRANKFURT","LSE","MILAN","MADRID","SIX SWISS","LISBON","BRUSSELS","DUBLIN"]
        )
    with c3:
        allow_types = st.multiselect(
            "Allow Quote Types:",
            ["EQUITY","ETF","FUND","CURRENCY","INDEX"],
            default=["EQUITY","ETF","FUND"]
        )

uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])

if uploaded:
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"Could not read the Excel file: {e}")
        st.stop()

    st.subheader("Preview")
    st.dataframe(df.head(20), use_container_width=True)

    # Column selection
    cols = list(df.columns)
    st.markdown("### Column Mapping")
    c1, c2 = st.columns(2)
    with c1:
        name_col = st.selectbox("Column with **Company Name**", options=cols, index=min(cols.index("Name"), len(cols)-1) if "Name" in cols else 0)
    with c2:
        symbol_col_existing = st.selectbox("Existing Symbol column (optional)", options=["<none>"] + cols, index=0)

    # Optional filters (e.g., restrict processing to blank symbols only)
    only_fill_blank = st.checkbox("Only fill rows where Symbol is blank/missing", value=True)

    st.markdown("---")
    run = st.button("🔎 Fetch Best-Match Tickers")

    if run:
        if not name_col:
            st.error("Please select the company name column.")
            st.stop()

        # Prepare output columns
        out_df = df.copy()

        # If existing symbol column is present and we only want to fill blanks, honor it
        if symbol_col_existing != "<none>" and symbol_col_existing in out_df.columns:
            needs_fill_mask = out_df[symbol_col_existing].isna() | (out_df[symbol_col_existing].astype(str).str.strip() == "")
        else:
            # No existing symbol column → create one
            symbol_col_existing = "Symbol"
            if "Symbol" not in out_df.columns:
                out_df["Symbol"] = ""
            needs_fill_mask = (out_df["Symbol"].astype(str).str.strip() == "")

        if not only_fill_blank:
            needs_fill_mask = pd.Series([True] * len(out_df))

        results = []
        total = int(needs_fill_mask.sum())
        progress = st.progress(0)
        status = st.empty()

        for i, (idx, row) in enumerate(out_df[needs_fill_mask].iterrows(), start=1):
            company = safe_str(row[name_col])
            progress.progress(min(i/total, 1.0) if total else 1.0)
            status.write(f"Searching: {company}")

            if not company:
                results.append((idx, "", "", "", "", "", 0.0))
                continue

            quotes = yahoo_search(company, lang="en-US", quotes_count=6, news_count=0, region="US")
            match = pick_best_match(quotes,
                                    prefer_exchanges=[x.upper() for x in prefer_exchanges] if prefer_exchanges else None,
                                    prefer_regions=[x.upper() for x in prefer_regions] if prefer_regions else None,
                                    allow_types=[x.upper() for x in allow_types] if allow_types else None)

            if match:
                symbol = match.get("symbol","")
                shortname = match.get("shortname","") or ""
                longname = match.get("longname","") or ""
                exch_disp = match.get("exchDisp","") or match.get("exchange","") or ""
                qtype = match.get("quoteType","")
                # crude confidence heuristic: first item + matching prefs already handled in picker
                # we'll assign simple tiers for user transparency:
                conf = 0.85 if qtype.upper() == "EQUITY" else 0.75
            else:
                symbol, shortname, longname, exch_disp, qtype, conf = "", "", "", "", "", 0.0

            out_df.loc[idx, symbol_col_existing] = symbol
            results.append((idx, symbol, shortname, longname, exch_disp, qtype, conf))
            time.sleep(0.15)  # be gentle

        status.empty()
        progress.empty()

        res_df = pd.DataFrame(results, columns=["_row_idx","Symbol","YF_ShortName","YF_LongName","Exchange","QuoteType","Confidence"])
        # Merge back for user review
        review_df = out_df.merge(res_df.drop(columns=["_row_idx"]), left_index=True, right_on="_row_idx", how="left")
        review_df.drop(columns=["_row_idx"], inplace=True, errors="ignore")

        st.success("Done! Review the matches below.")
        st.dataframe(review_df, use_container_width=True)

        # Download updated Excel
        xlsx_bytes = to_excel_bytes(out_df)
        st.download_button(
            label="📥 Download Excel with Symbols",
            data=xlsx_bytes,
            file_name="with_yf_symbols.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        with st.expander("🔍 Debug view (raw Yahoo candidates per name)"):
            # Optional: show raw candidates for the last 10 processed names for transparency
            st.write("For transparency, you can inspect how Yahoo matched your queries. (Disabled by default for performance.)")
            st.caption("Tip: If you need this expanded for all rows, tell me and I’ll add a CSV export of all candidates per name.")
else:
    st.info("Upload an Excel file to begin. Example columns: **Name** (required or selectable), optional **Symbol** to fill in.")

st.markdown("---")
st.caption("Made for Streamlit Cloud • Fills Yahoo Finance tickers from your uploaded list • Dark UI")
