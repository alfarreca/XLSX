# fill_yf_sector.py
import io
import re
import time
import math
import logging
import random
import pandas as pd
import streamlit as st

# ---------- Config ----------
KNOWN_EX_SUFFIXES = {
    ".TO",".V",".CN",".AX",".L",".SW",".PA",".BR",".BE",".DE",".F",".HM",".MU",".SG",
    ".ST",".HE",".MI",".AS",".MC",".WA",".VI",".IR",".IS",".HK",".KS",".KQ",".T",".TW"
}
DEFAULT_TICKER_COL = "Symbol"
DEFAULT_EXCHANGE_COL = "Exchange"          # input column (optional, helps mapping)
DEFAULT_SECTOR_COL = "Sector (YF)"         # output
DEFAULT_INDUSTRY_COL = "Industry (YF)"     # output
DEFAULT_EXCHANGE_OUT_COL = "Exchange (YF)" # output (new)
MAX_SHEET_ROWS_HARD_CAP = 500
# ----------------------------

st.set_page_config(page_title="Fill Sector/Industry from Yahoo Finance", layout="wide")
st.title("📊 Fill Sector/Industry from Yahoo Finance")

with st.sidebar:
    st.header("Settings")
    # Inputs
    ticker_col_name = st.text_input("Ticker column name", value=DEFAULT_TICKER_COL)
    exchange_col_name = st.text_input("Input column: Exchange (optional, helps mapping US vs non-US)", value=DEFAULT_EXCHANGE_COL)

    # Outputs
    sector_col_name = st.text_input("Output column: Sector", value=DEFAULT_SECTOR_COL)
    industry_col_name = st.text_input("Output column: Industry", value=DEFAULT_INDUSTRY_COL)
    exchange_out_col_name = st.text_input("Output column: Exchange", value=DEFAULT_EXCHANGE_OUT_COL)

    skip_filled = st.checkbox("Skip rows already filled (Sector & Industry both present)", value=True)
    request_delay = st.number_input("Delay per request (seconds)", value=0.70, step=0.1, min_value=0.0)
    max_retries = st.number_input("Max retries per ticker", value=1, step=1, min_value=0)
    checkpoint_every = st.number_input("Checkpoint save every N rows", value=50, step=10, min_value=10)
    do_exists_check = st.checkbox("Pre-check ticker exists (use only for cleanup)", value=False)

    st.markdown("---")
    user_sheet_rows = st.number_input("Max rows per sheet (≤ 500)", value=500, step=50, min_value=100, max_value=500)
    max_rows_per_sheet = min(int(user_sheet_rows), MAX_SHEET_ROWS_HARD_CAP)

    # Anti-burst
    jitter_pct = st.slider("Jitter ±% around delay (reduces burst 404s)", min_value=0, max_value=30, value=10)

    st.caption("Tip: If you see 404 spam, increase delay and/or enable jitter.")

uploaded = st.file_uploader("Upload your Excel file", type=["xlsx"])
sheet_name = None

logging.getLogger("yfinance").setLevel(logging.ERROR)

def write_df_paged(_df: pd.DataFrame, writer, page_size: int = 500):
    """Write DataFrame across multiple sheets, each with up to `page_size` rows."""
    n = len(_df)
    if n == 0:
        _df.to_excel(writer, index=False, sheet_name="Data_1")
        return
    pages = (n + page_size - 1) // page_size
    for p in range(pages):
        start = p * page_size
        end = min(start + page_size, n)
        sheet = f"Data_{p+1}"
        _df.iloc[start:end].to_excel(writer, index=False, sheet_name=sheet)

def ensure_string_cols(df: pd.DataFrame, cols):
    """Force columns to pandas 'string' dtype to avoid dtype warnings on assignment."""
    for c in cols:
        if c not in df.columns:
            df[c] = pd.Series(dtype="string")
        # If column exists but is not string, cast safely
        if pd.api.types.infer_dtype(df[c], skipna=True) != "string":
            try:
                df[c] = df[c].astype("string")
            except Exception:
                # Last resort: to object
                df[c] = df[c].astype(object)

if uploaded:
    try:
        xl = pd.ExcelFile(uploaded)
        sheet_name = xl.sheet_names[0] if len(xl.sheet_names) == 1 else st.selectbox("Choose a sheet", xl.sheet_names, index=0)
        df = xl.parse(sheet_name)
    except Exception as e:
        st.error(f"Could not read Excel: {e}")
        st.stop()

    if ticker_col_name not in df.columns:
        st.error(f"Column '{ticker_col_name}' not found. Available: {list(df.columns)}")
        st.stop()

    # Ensure output columns exist and are STRING dtype (prevents FutureWarning)
    for col in [sector_col_name, industry_col_name, "Name", "Country", "Asset_Type", exchange_out_col_name]:
        if col not in df.columns:
            df[col] = pd.Series(dtype="string")
    ensure_string_cols(df, [sector_col_name, industry_col_name, "Name", "Country", "Asset_Type", exchange_out_col_name])

    with st.expander("Preview uploaded data"):
        st.dataframe(df.head(20), use_container_width=True)

    # Lazy import yfinance so UI renders even if missing
    try:
        import yfinance as yf
        from urllib.error import HTTPError
    except Exception:
        st.error("`yfinance` is not installed. Add to requirements.txt:\n\n`yfinance>=0.2.40`")
        st.stop()

    # --- Mapping for US class-share tickers (BRK.B -> BRK-B); preserve real exchange suffixes (.L, .TO, etc.)
    class_share_pat = re.compile(r"^[A-Z]+\.([A-Z0-9]{1,3})$")

    def map_to_yahoo_symbol(symbol, exchange):
        sym = str(symbol).strip().upper() if symbol is not None else ""
        if not sym:
            return sym
        for suf in KNOWN_EX_SUFFIXES:
            if sym.endswith(suf):
                return sym  # keep native exchange suffix as-is

        is_us = False
        if exchange is not None:
            ex = str(exchange).strip().upper()
            if any(k in ex for k in ("NYSE", "NASDAQ", "NSDQ", "OTC", "ARCA", "BATS", "AMEX", "NYSEMKT", "NMS")):
                is_us = True
        if (exchange in (None, "", "nan")) and class_share_pat.match(sym):
            is_us = True

        if is_us and "." in sym:
            base, tail = sym.split(".", 1)
            if re.fullmatch(r"[A-Z0-9]{1,3}", tail):
                return f"{base}-{tail}"
        return sym

    @st.cache_data(ttl=60*60*24, show_spinner=False)
    def cached_get_info(yf_symbol):
        """
        Cached metadata fetch with layered fallbacks and 404 suppression.
        Returns dict: sector, industry, name, country, asset_type, exchange.
        """
        try:
            t = yf.Ticker(yf_symbol)

            info = {}
            # 1) Preferred: get_info (new API)
            try:
                info = t.get_info()
                if not isinstance(info, dict):
                    info = {}
            except HTTPError as he:
                # Ignore noisy 404s from Yahoo endpoints
                if getattr(he, "code", None) != 404:
                    raise
                info = {}
            except Exception:
                info = {}

            # 2) Legacy property .info as fallback
            if not info:
                try:
                    maybe = getattr(t, "info", {}) or {}
                    if isinstance(maybe, dict):
                        info = maybe
                except Exception:
                    pass

            # Extract from info if available
            sector   = info.get("sector")
            industry = info.get("industry") or info.get("industryKey") or info.get("industryDisp")
            name     = info.get("shortName") or info.get("longName")
            country  = info.get("country")
            asset_tp = info.get("quoteType")
            exch_yf  = info.get("exchange") or info.get("market")

            # 3) fast_info supplement
            fi = getattr(t, "fast_info", None)
            try:
                if fi and not exch_yf:
                    exch_yf = getattr(fi, "market", None)
            except Exception:
                pass

            # 4) Light history ping to validate existence (no need to parse values)
            if not any([sector, industry, name, country, asset_tp, exch_yf]):
                try:
                    hist = t.history(period="5d", interval="1d", auto_adjust=False)
                    if hist is not None and not hist.empty:
                        # If we reached here, set at least exchange if fast_info has it
                        if fi and not exch_yf:
                            exch_yf = getattr(fi, "market", None)
                except Exception:
                    pass

            # Normalize/clean
            norm = lambda x: x if (x is not None and str(x).strip()) else None
            return {
                "sector":   norm(sector),
                "industry": norm(industry),
                "name":     norm(name),
                "country":  norm(country),
                "asset_type": norm(asset_tp),
                "exchange": norm(exch_yf),
            }
        except Exception:
            return {}

    @st.cache_data(ttl=60*60*24, show_spinner=False)
    def cached_exists(yf_symbol):
        """Conservative existence check that FAILS OPEN."""
        import yfinance as yf
        try:
            t = yf.Ticker(yf_symbol)
            try:
                info = t.get_info()
                if isinstance(info, dict) and (
                    info.get("quoteType") or info.get("shortName") or info.get("longName") or info.get("sector")
                ):
                    return True
            except Exception:
                pass
            try:
                hist = t.history(period="5d", interval="1d", auto_adjust=False)
                if hist is not None and not hist.empty:
                    return True
            except Exception:
                pass
            fi = getattr(t, "fast_info", None)
            try:
                if fi and (getattr(fi, "last_price", None) is not None or getattr(fi, "market", None) is not None):
                    return True
            except Exception:
                pass
            return True
        except Exception:
            return True

    # Build worklist (skip logic: both sector & industry already present)
    work_indices = []
    for i, row in df.iterrows():
        sym = str(row.get(ticker_col_name, "")).strip()
        if not sym:
            continue
        if skip_filled:
            filled_sector = df.at[i, sector_col_name]
            filled_ind = df.at[i, industry_col_name]
            if (isinstance(filled_sector, str) and filled_sector.strip()) and (isinstance(filled_ind, str) and filled_ind.strip()):
                continue
        work_indices.append(i)

    st.write(f"Tickers to process: **{len(work_indices)}** (of {len(df)})")

    # Session storage for live checkpoint
    if "partial_bytes" not in st.session_state:
        st.session_state.partial_bytes = None
    if "logs" not in st.session_state:
        st.session_state.logs = []

    def save_checkpoint(_df: pd.DataFrame):
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            write_df_paged(_df, writer, page_size=max_rows_per_sheet)
        buf.seek(0)
        st.session_state.partial_bytes = buf.getvalue()

    # Optional: single-ticker tester
    with st.expander("🔎 Single-Ticker Tester"):
        test_sym = st.text_input("Symbol (e.g., BRK.B, SHOP.TO, AI)", "")
        test_ex = st.text_input("Input Exchange (optional, helps US vs non-US mapping)", "")
        if st.button("Test mapping & fetch", use_container_width=False):
            if test_sym:
                mapped = map_to_yahoo_symbol(test_sym, test_ex)
                st.write(f"Mapped → **{mapped}**")
                if do_exists_check:
                    st.write(f"Exists check: **{cached_exists(mapped)}**")
                meta = cached_get_info(mapped)
                st.write(
                    f"Name: **{meta.get('name')}** • Country: **{meta.get('country')}** • "
                    f"Type: **{meta.get('asset_type')}** • Exchange: **{meta.get('exchange')}**"
                )
                st.write(
                    f"Sector: **{meta.get('sector')}**, Industry: **{meta.get('industry')}**"
                )

    colA, colB = st.columns([1,1])
    start = colA.button("▶️ Start Filling from Yahoo")
    clear_logs = colB.button("🧹 Clear logs")
    if clear_logs:
        st.session_state.logs = []
        st.session_state.partial_bytes = None
        st.info("Logs and checkpoint cleared.")

    def sleep_with_jitter(base, pct):
        if base <= 0 or pct <= 0:
            time.sleep(max(base, 0))
            return
        delta = base * (pct / 100.0)
        wait = base + random.uniform(-delta, delta)
        time.sleep(max(wait, 0))

    if start and work_indices:
        progress = st.progress(0)
        status = st.empty()
        processed = 0
        failures = 0
        err_rows = []

        for k, idx in enumerate(work_indices, start=1):
            sym = str(df.at[idx, ticker_col_name]).strip()
            exch_in = str(df.at[idx, exchange_col_name]).strip() if exchange_col_name in df.columns else None
            yf_sym = map_to_yahoo_symbol(sym, exch_in)

            # Optional quick existence check (fails open; rarely returns False)
            if do_exists_check and not cached_exists(yf_sym):
                failures += 1
                err_rows.append({
                    "Symbol": sym, "Mapped": yf_sym, "Status": "not_found",
                    "Sector": None, "Industry": None, "Name": None, "Country": None, "Asset_Type": None,
                    exchange_out_col_name: None, "Error": "existence_check_failed"
                })
                status.write(f"❌ {k}/{len(work_indices)} • {sym} → {yf_sym} • not found (pre-check)")
                if k % int(checkpoint_every) == 0:
                    save_checkpoint(df)
                processed += 1
                progress.progress(int(100 * processed / len(work_indices)))
                sleep_with_jitter(request_delay, jitter_pct)
                continue

            # Fetch with retries (exponential backoff + jitter)
            last_err_msg = None
            result = {}
            for attempt in range(int(max_retries) + 1):
                try:
                    result = cached_get_info(yf_sym)
                    if any(result.get(x) for x in ("sector","industry","name","country","asset_type","exchange")):
                        break
                except Exception as e:
                    last_err_msg = str(e)
                # backoff (0.4s base like before, grows per attempt)
                time.sleep(0.4 * (attempt + 1))
            sector      = result.get("sector")
            industry    = result.get("industry")
            name        = result.get("name")
            country     = result.get("country")
            asset_type  = result.get("asset_type")
            exchange_yf = result.get("exchange")

            # Assign (columns already string dtype)
            if isinstance(sector, str) and sector.strip():
                df.at[idx, sector_col_name] = sector.strip()
            if isinstance(industry, str) and industry.strip():
                df.at[idx, industry_col_name] = industry.strip()
            if isinstance(name, str) and name.strip():
                df.at[idx, "Name"] = name.strip()
            if isinstance(country, str) and country.strip():
                df.at[idx, "Country"] = country.strip()
            if isinstance(asset_type, str) and asset_type.strip():
                df.at[idx, "Asset_Type"] = asset_type.strip()
            if isinstance(exchange_yf, str) and exchange_yf.strip():
                df.at[idx, exchange_out_col_name] = exchange_yf.strip()

            ok = any([
                isinstance(sector, str) and sector.strip(),
                isinstance(industry, str) and industry.strip(),
                isinstance(name, str) and name.strip(),
                isinstance(country, str) and country.strip(),
                isinstance(asset_type, str) and asset_type.strip(),
                isinstance(exchange_yf, str) and exchange_yf.strip()
            ])

            if not ok:
                failures += 1
                err_rows.append({
                    "Symbol": sym, "Mapped": yf_sym, "Status": "empty",
                    "Sector": sector, "Industry": industry, "Name": name, "Country": country,
                    "Asset_Type": asset_type, exchange_out_col_name: exchange_yf,
                    "Error": last_err_msg
                })

            processed += 1
            progress.progress(int(100 * processed / len(work_indices)))
            prefix = "✅" if ok else "⚠️"
            status.write(
                f"{prefix} {k}/{len(work_indices)} • {sym} → {yf_sym} • "
                f"Name='{name}' Country='{country}' Type='{asset_type}' {exchange_out_col_name}='{exchange_yf}' • "
                f"Sector='{sector}' Industry='{industry}'"
            )

            # Delay (with jitter)
            sleep_with_jitter(request_delay, jitter_pct)

            # Checkpoint
            if k % int(checkpoint_every) == 0:
                save_checkpoint(df)
                st.info(f"Checkpoint saved at {k} rows.")

        # Final checkpoint (multi-sheet)
        save_checkpoint(df)
        st.success(f"Done. Processed {processed} rows. Empty/failed: {failures}")

        if err_rows:
            logs_df = pd.DataFrame(err_rows)
            st.session_state.logs = err_rows
            st.subheader("Errors / Empty Results")
            st.dataframe(logs_df.head(200), use_container_width=True)

            csv_buf = io.StringIO()
            logs_df.to_csv(csv_buf, index=False)
            st.download_button(
                "⬇️ Download error log (CSV)",
                data=csv_buf.getvalue(),
                file_name="yf_sector_errors.csv",
                mime="text/csv",
            )

        st.subheader("Sample of Updated Data")
        st.dataframe(df.head(50), use_container_width=True)

        if st.session_state.partial_bytes:
            st.download_button(
                label=f"⬇️ Download updated Excel (paged ≤ {max_rows_per_sheet}/sheet)",
                data=st.session_state.partial_bytes,
                file_name="with_yf_sectors.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    elif start and not work_indices:
        st.info("Nothing to do: no rows need filling (or outputs already filled).")
else:
    st.info("Upload your Excel to begin.")
