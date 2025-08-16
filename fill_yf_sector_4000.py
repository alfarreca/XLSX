# fill_yf_sector_3000.py
# Streamlit app to:
#  - Upload an XLSX
#  - Auto-fill empty 'Symbol' from 'Name' using Yahoo search with validation
#  - Optionally fetch Sector/Industry from Yahoo (yahooquery / yfinance)
#  - Apply pre-fetch filters
#  - Export to XLSX (≤ N rows per sheet)

import io
import math
import time
from typing import Optional, Tuple, Dict, Any

import pandas as pd
import streamlit as st

# yfinance for validation / fallback
import yfinance as yf

# yahooquery for searching and richer profile data
try:
    from yahooquery import Ticker as YQTicker
    from yahooquery import search as yq_search
    _HAS_YQ = True
except Exception:
    _HAS_YQ = False


# ----------------------------
# Streamlit Page Config
# ----------------------------
st.set_page_config(page_title="Auto-Fill Symbols + Sector/Industry", layout="wide")


# ----------------------------
# Utilities
# ----------------------------
@st.cache_data(show_spinner=False)
def strict_exists(symbol: str) -> bool:
    """
    'Strict' existence validator to avoid false positives from search.
    Tries quick checks first; falls back to light history probe.
    """
    try:
        if not symbol or not str(symbol).strip():
            return False

        t = yf.Ticker(symbol)
        # Try fast_info first (very light)
        try:
            fi = getattr(t, "fast_info", None)
            if fi and isinstance(fi, dict):
                # A tradable should have at least one of these fields
                if any(k in fi for k in ("lastPrice", "last_price", "currency", "marketCap", "market_cap")):
                    return True
        except Exception:
            pass

        # Try very small history call; if it returns rows, it's live enough
        try:
            hist = t.history(period="5d", interval="1d")
            if isinstance(hist, pd.DataFrame) and len(hist) > 0:
                return True
        except Exception:
            pass

        return False
    except Exception:
        return False


def _score_quote(q: Dict[str, Any], country_hint: Optional[str], exchange_hint: Optional[str]) -> float:
    s = 0.0
    qt = (q.get("quoteType") or "").upper()
    if qt in ("EQUITY", "ETF", "MUTUALFUND"):
        s += 3
    if country_hint and str(q.get("country", "")).strip().upper() == str(country_hint).strip().upper():
        s += 2
    if exchange_hint and str(q.get("exchDisp", "")).strip().upper().startswith(str(exchange_hint).strip().upper()):
        s += 2
    try:
        s += float(q.get("score", 0) or 0)
    except Exception:
        pass
    return s


def _best_symbol_from_quotes(quotes, country_hint=None, exchange_hint=None) -> Optional[str]:
    if not quotes:
        return None
    # filter out obvious non-tradables like futures suffixed '=F'
    filt = [q for q in quotes if q.get("symbol") and not str(q.get("symbol")).endswith("=F")]
    if not filt:
        filt = quotes
    best = max(filt, key=lambda q: _score_quote(q, country_hint, exchange_hint))
    return best.get("symbol")


@st.cache_data(show_spinner=False)
def resolve_symbol_from_name(name: str,
                             country_hint: Optional[str] = None,
                             exchange_hint: Optional[str] = None) -> Optional[str]:
    """Use yahooquery search to find best ticker from a company/fund name and validate it."""
    if not _HAS_YQ or not name or not str(name).strip():
        return None
    try:
        res = yq_search(str(name).strip())
        quotes = res.get("quotes", []) if isinstance(res, dict) else []
        cand = _best_symbol_from_quotes(quotes, country_hint, exchange_hint)
        if not cand:
            return None
        return cand if strict_exists(cand) else None
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def fetch_sector_industry(symbol: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Try yahooquery first (assetProfile), then fall back to yfinance.info.
    Returns (sector, industry) or (None, None).
    """
    sector = None
    industry = None

    # yahooquery path
    if _HAS_YQ:
        try:
            yqt = YQTicker(symbol, asynchronous=False)
            ap = yqt.asset_profile
            # asset_profile can be dict for single symbol, or dict of dicts for multiple
            if isinstance(ap, dict):
                # normalize to single-symbol dict
                row = ap.get(symbol) if symbol in ap else ap
                if isinstance(row, dict):
                    sector = row.get("sector")
                    industry = row.get("industry")
        except Exception:
            pass

    # fallback to yfinance
    if not sector or not industry:
        try:
            info = yf.Ticker(symbol).info or {}
            sector = sector or info.get("sector")
            industry = industry or info.get("industry")
        except Exception:
            pass

    # normalize blanks to None
    sector = sector if (sector and str(sector).strip()) else None
    industry = industry if (industry and str(industry).strip()) else None
    return sector, industry


def _coerce_str_series(s: pd.Series) -> pd.Series:
    return s.astype("string").fillna("").apply(lambda x: x.strip() if isinstance(x, str) else x)


def _maybe_add_column(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        df[col] = pd.Series(dtype="string")


def to_excel_bytes_multi(df: pd.DataFrame, sheet_base: str = "Sheet", max_rows_per_sheet: int = 3000) -> bytes:
    """
    Save a DataFrame into an XLSX, splitting across multiple sheets
    if df has more than max_rows_per_sheet rows.
    """
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if len(df) <= max_rows_per_sheet:
            df.to_excel(writer, index=False, sheet_name=sheet_base)
        else:
            n_sheets = math.ceil(len(df) / max_rows_per_sheet)
            for i in range(n_sheets):
                start = i * max_rows_per_sheet
                end = min((i + 1) * max_rows_per_sheet, len(df))
                part = df.iloc[start:end].copy()
                part.to_excel(writer, index=False, sheet_name=f"{sheet_base}_{i+1}")
    buf.seek(0)
    return buf.getvalue()


# ----------------------------
# Sidebar Controls
# ----------------------------
with st.sidebar:
    st.header("Settings")

    st.markdown("**Columns (set to match your sheet):**")
    symbol_col = st.text_input("Symbol column", value="Symbol")
    name_col = st.text_input("Name column", value="Name")
    sector_col = st.text_input("Sector (YF) column", value="Sector (YF)")
    industry_col = st.text_input("Industry (YF) column", value="Industry (YF)")
    country_hint_col = st.text_input("Country hint column (optional)", value="Country")
    exchange_hint_col = st.text_input("Exchange hint column (optional)", value="Exchange")

    st.divider()
    st.subheader("Symbol Auto-Fill")
    auto_fill_symbol = st.checkbox("Auto-fill empty 'Symbol' from Name", value=True)

    st.divider()
    st.subheader("Fetch Sector / Industry")
    do_fetch_si = st.checkbox("Fetch/Update Sector & Industry from Yahoo", value=True)
    only_fill_missing_si = st.checkbox("Only fill missing Sector/Industry", value=True)

    st.divider()
    st.subheader("Pre-Fetch Filters (apply to rows that will be processed)")
    filter_country = st.text_input("Filter by Country equals (optional)", value="")
    filter_name_contains = st.text_input("Filter Name contains (optional)", value="")
    fetch_only_filtered = st.checkbox("Fetch only for filtered rows", value=False)

    st.divider()
    st.subheader("Excel Export")
    max_rows_per_sheet = st.number_input("Max rows per sheet (≤ 3000 recommended)",
                                         min_value=500, max_value=100000,
                                         value=3000, step=100)

# ----------------------------
# Main App
# ----------------------------
st.title("XLSX: Auto-Fill Symbols from Names + Sector/Industry")

uploaded = st.file_uploader("Upload your Excel (.xlsx)", type=["xlsx"])
if not uploaded:
    st.info("Upload an Excel file to begin.")
    st.stop()

try:
    df = pd.read_excel(uploaded)
except Exception as e:
    st.error(f"Failed to read Excel: {e}")
    st.stop()

st.success(f"Loaded sheet with {len(df):,} rows and {len(df.columns)} columns.")

# Ensure critical columns exist
_maybe_add_column(df, symbol_col)
_maybe_add_column(df, name_col)
_maybe_add_column(df, sector_col)
_maybe_add_column(df, industry_col)
if country_hint_col and country_hint_col not in df.columns:
    df[country_hint_col] = pd.Series(dtype="string")
if exchange_hint_col and exchange_hint_col not in df.columns:
    df[exchange_hint_col] = pd.Series(dtype="string")

# Normalize types
df[symbol_col] = _coerce_str_series(df[symbol_col])
df[name_col] = _coerce_str_series(df[name_col])

# Show a preview
st.subheader("Preview (first 10 rows)")
st.dataframe(df.head(10), use_container_width=True)

# Build filtered view (does not mutate df yet)
mask = pd.Series(True, index=df.index)
if filter_country.strip() and country_hint_col in df.columns:
    mask &= df[country_hint_col].astype("string").str.fullmatch(filter_country.strip(), case=False, na=False)
if filter_name_contains.strip():
    mask &= df[name_col].astype("string").str.contains(filter_name_contains.strip(), case=False, na=False)

filtered_df = df.loc[mask].copy()
st.caption(f"Filtered rows: {len(filtered_df):,} / {len(df):,}")

# ----------------------------
# Actions
# ----------------------------
do_run = st.button("Run Auto-Fill + Fetch", type="primary")

if do_run:
    progress = st.empty()
    log = st.container()

    # 1) Auto-fill Symbol from Name (validated)
    if auto_fill_symbol:
        if not _HAS_YQ:
            st.warning("Symbol auto-fill requested but `yahooquery` is not available. Add `yahooquery>=2.3.7` to requirements.")
        else:
            rows = filtered_df.index if fetch_only_filtered else df.index
            filled, unresolved, skipped = 0, 0, 0

            for idx_i, i in enumerate(rows, start=1):
                progress.progress(min(100, int(idx_i / max(1, len(rows)) * 100)),
                                  text=f"Auto-filling Symbols {idx_i}/{len(rows)}")
                sym_now = str(df.at[i, symbol_col] or "").strip()
                nm = str(df.at[i, name_col] or "").strip()

                # Skip if already has symbol or no name
                if sym_now or not nm:
                    skipped += 1
                    continue

                country_hint = df.at[i, country_hint_col] if country_hint_col in df.columns else None
                exch_hint = df.at[i, exchange_hint_col] if exchange_hint_col in df.columns else None

                cand = resolve_symbol_from_name(nm, country_hint, exch_hint)
                if cand:
                    df.at[i, symbol_col] = cand
                    filled += 1
                else:
                    unresolved += 1

            with log:
                st.info(f"Symbol auto-fill complete → filled: **{filled}**, unresolved: **{unresolved}**, skipped: **{skipped}**.")

    # 2) Fetch Sector / Industry
    if do_fetch_si:
        # Define which rows to update
        rows = filtered_df.index if fetch_only_filtered else df.index
        updated, untouched = 0, 0

        for idx_i, i in enumerate(rows, start=1):
            progress.progress(min(100, int(idx_i / max(1, len(rows)) * 100)),
                              text=f"Fetching Sector/Industry {idx_i}/{len(rows)}")

            sym = str(df.at[i, symbol_col] or "").strip()
            if not sym:
                untouched += 1
                continue

            if only_fill_missing_si:
                has_sector = bool(str(df.at[i, sector_col] or "").strip())
                has_ind = bool(str(df.at[i, industry_col] or "").strip())
                if has_sector and has_ind:
                    untouched += 1
                    continue

            sector, industry = fetch_sector_industry(sym)
            wrote = False
            if sector and (not only_fill_missing_si or not str(df.at[i, sector_col] or "").strip()):
                df.at[i, sector_col] = sector
                wrote = True
            if industry and (not only_fill_missing_si or not str(df.at[i, industry_col] or "").strip()):
                df.at[i, industry_col] = industry
                wrote = True

            if wrote:
                updated += 1
            else:
                untouched += 1

            # Be nice to APIs
            time.sleep(0.05)

        with log:
            st.info(f"Sector/Industry update → updated: **{updated}**, untouched/skipped: **{untouched}**.")

    progress.empty()

    # Show result preview
    st.subheader("Result Preview (first 20 rows)")
    st.dataframe(df.head(20), use_container_width=True)

    # Download button
    out_bytes = to_excel_bytes_multi(df, sheet_base="Updated", max_rows_per_sheet=int(max_rows_per_sheet))
    st.download_button(
        label="Download updated Excel",
        data=out_bytes,
        file_name="updated_symbols_and_metadata.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    st.success("Done!")


# ----------------------------
# Footer notes
# ----------------------------
st.markdown(
    """
    <small>
    Tips:
    <ul>
      <li>For better symbol resolution, fill optional <b>Country</b> and <b>Exchange</b> columns (e.g., "United States", "NASDAQ").</li>
      <li>Leave <b>fetch-only-filtered</b> off to process the whole sheet; turn it on to iterate faster on a subset first.</li>
      <li>If a name is ambiguous, unresolved rows are safer than wrong tickers—review and retry with better hints.</li>
    </ul>
    </small>
    """,
    unsafe_allow_html=True
)
