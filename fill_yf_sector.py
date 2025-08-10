import io
import re
import time
import logging
import pandas as pd
import streamlit as st

# ---------- Config ----------
KNOWN_EX_SUFFIXES = {
    ".TO",".V",".CN",".AX",".L",".SW",".PA",".BR",".BE",".DE",".F",".HM",".MU",".SG",
    ".ST",".HE",".MI",".AS",".MC",".WA",".VI",".IR",".IS",".HK",".KS",".KQ",".T",".TW"
}
DEFAULT_TICKER_COL = "Symbol"
DEFAULT_EXCHANGE_COL = "Exchange"
DEFAULT_SECTOR_COL = "Sector (YF)"
DEFAULT_INDUSTRY_COL = "Industry (YF)"
# ----------------------------

st.set_page_config(page_title="Fill Sector/Industry from Yahoo Finance", layout="wide")
st.title("📊 Fill Sector/Industry from Yahoo Finance")

with st.sidebar:
    st.header("Settings")
    ticker_col_name = st.text_input("Ticker column name", value=DEFAULT_TICKER_COL)
    exchange_col_name = st.text_input("Exchange column (optional)", value=DEFAULT_EXCHANGE_COL)
    sector_col_name = st.text_input("Output column: Sector", value=DEFAULT_SECTOR_COL)
    industry_col_name = st.text_input("Output column: Industry", value=DEFAULT_INDUSTRY_COL)
    skip_filled = st.checkbox("Skip rows already filled (both columns)", value=True)
    request_delay = st.number_input("Delay per request (seconds)", value=0.7, step=0.1, min_value=0.0)
    max_retries = st.number_input("Max retries per ticker", value=1, step=1, min_value=0)
    checkpoint_every = st.number_input("Checkpoint save every N rows", value=50, step=10, min_value=10)
    do_exists_check = st.checkbox("Pre-check ticker exists (use only for cleanup)", value=False)
    st.caption("Tip: Increase delay if you hit Yahoo rate limits.")

uploaded = st.file_uploader("Upload your Excel file", type=["xlsx"])
sheet_name = None

logging.getLogger("yfinance").setLevel(logging.ERROR)

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

    # Ensure output columns exist
    if sector_col_name not in df.columns:
        df[sector_col_name] = None
    if industry_col_name not in df.columns:
        df[industry_col_name] = None

    with st.expander("Preview uploaded data"):
        st.dataframe(df.head(20), use_container_width=True)

    # Lazy import yfinance so UI renders even if missing
    try:
        import yfinance as yf
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
        if exchange in (None, "", "nan") and class_share_pat.match(sym):
            is_us = True

        if is_us and "." in sym:
            base, tail = sym.split(".", 1)
            if re.fullmatch(r"[A-Z0-9]{1,3}", tail):
                return f"{base}-{tail}"
        return sym

    @st.cache_data(ttl=60*60*24, show_spinner=False)
    def cached_get_info(yf_symbol):
        """
        Cached metadata fetch. Returns dict {'sector':..., 'industry':...}.
        Returns {} on failure; never raises.
        """
        try:
            t = yf.Ticker(yf_symbol)
            info = {}
            try:
                info = t.get_info()
            except Exception:
                try:
                    info = getattr(t, "info", {}) or {}
                except Exception:
                    info = {}
            if not isinstance(info, dict):
                return {}
            sector = info.get("sector")
            industry = info.get("industry") or info.get("industryKey") or info.get("industryDisp")
            sector = sector if sector and str(sector).strip() else None
            industry = industry if industry and str(industry).strip() else None
            return {"sector": sector, "industry": industry}
        except Exception:
            return {}

    @st.cache_data(ttl=60*60*24, show_spinner=False)
    def cached_exists(yf_symbol):
        """
        Conservative existence check that FAILS OPEN.
        Only returns False if we can clearly confirm non-existence across methods.
        Otherwise returns True to avoid false negatives on Streamlit Cloud.
        """
        import yfinance as yf
        try:
            t = yf.Ticker(yf_symbol)

            # 1) get_info has some basic keys for live symbols
            try:
                info = t.get_info()
                if isinstance(info, dict) and (
                    info.get("quoteType") or info.get("shortName") or info.get("longName") or info.get("sector")
                ):
                    return True
            except Exception:
                pass

            # 2) recent history
            try:
                hist = t.history(period="5d", interval="1d", auto_adjust=False)
                if hist is not None and not hist.empty:
                    return True
            except Exception:
                pass

            # 3) fast_info (use only to confirm, never to deny)
            fi = getattr(t, "fast_info", None)
            try:
                if fi:
                    last_price = getattr(fi, "last_price", None)
                    market = getattr(fi, "market", None)
                    if last_price is not None or market is not None:
                        return True
            except Exception:
                pass

            # Could not confirm either way → fail open
            return True
        except Exception:
            return True

    # Build worklist
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
            _df.to_excel(writer, index=False)
        buf.seek(0)
        st.session_state.partial_bytes = buf.getvalue()

    # Optional: single-ticker tester
    with st.expander("🔎 Single-Ticker Tester"):
        test_sym = st.text_input("Symbol (e.g., BRK.B, SHOP.TO, AI)", "")
        test_ex = st.text_input("Exchange (optional, helps for US vs non-US)", "")
        if st.button("Test mapping & fetch", use_container_width=False):
            if test_sym:
                mapped = map_to_yahoo_symbol(test_sym, test_ex)
                st.write(f"Mapped → **{mapped}**")
                if do_exists_check:
                    st.write(f"Exists check: **{cached_exists(mapped)}**")
                meta = cached_get_info(mapped)
                st.write(f"Sector: **{meta.get('sector')}**, Industry: **{meta.get('industry')}**")

    colA, colB = st.columns([1,1])
    start = colA.button("▶️ Start Filling from Yahoo")
    clear_logs = colB.button("🧹 Clear logs")
    if clear_logs:
        st.session_state.logs = []
        st.session_state.partial_bytes = None
        st.info("Logs and checkpoint cleared.")

    if start and work_indices:
        progress = st.progress(0)
        status = st.empty()
        processed = 0
        failures = 0
        err_rows = []

        for k, idx in enumerate(work_indices, start=1):
            sym = str(df.at[idx, ticker_col_name]).strip()
            exch = str(df.at[idx, exchange_col_name]).strip() if exchange_col_name in df.columns else None
            yf_sym = map_to_yahoo_symbol(sym, exch)

            # Optional quick existence check (fails open; rarely returns False)
            if do_exists_check and not cached_exists(yf_sym):
                failures += 1
                err_rows.append({"Symbol": sym, "Mapped": yf_sym, "Status": "not_found", "Sector": None, "Industry": None, "Error": "existence_check_failed"})
                status.write(f"❌ {k}/{len(work_indices)} • {sym} → {yf_sym} • not found (pre-check)")
                if k % int(checkpoint_every) == 0:
                    save_checkpoint(df)
                processed += 1
                progress.progress(int(100 * processed / len(work_indices)))
                time.sleep(request_delay)
                continue

            # Fetch with retries
            last_err_msg = None
            result = {}
            for attempt in range(int(max_retries) + 1):
                try:
                    result = cached_get_info(yf_sym)
                    if result.get("sector") or result.get("industry"):
                        break
                except Exception as e:
                    last_err_msg = str(e)
                time.sleep(0.4 * (attempt + 1))

            sector = result.get("sector")
            industry = result.get("industry")

            # Assign only explicit strings to avoid FutureWarnings
            if isinstance(sector, str) and sector.strip():
                df.at[idx, sector_col_name] = sector.strip()
            if isinstance(industry, str) and industry.strip():
                df.at[idx, industry_col_name] = industry.strip()

            ok = bool((isinstance(sector, str) and sector.strip()) or (isinstance(industry, str) and industry.strip()))
            if not ok:
                failures += 1
                err_rows.append({
                    "Symbol": sym, "Mapped": yf_sym, "Status": "empty",
                    "Sector": sector, "Industry": industry, "Error": last_err_msg
                })

            processed += 1
            progress.progress(int(100 * processed / len(work_indices)))
            prefix = "✅" if ok else "⚠️"
            status.write(f"{prefix} {k}/{len(work_indices)} • {sym} → {yf_sym} • Sector='{sector}' Industry='{industry}'")

            time.sleep(request_delay)

            # Checkpoint
            if k % int(checkpoint_every) == 0:
                save_checkpoint(df)
                st.info(f"Checkpoint saved at {k} rows.")

        # Final checkpoint
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
                label="⬇️ Download updated Excel",
                data=st.session_state.partial_bytes,
                file_name="with_yf_sectors.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    elif start and not work_indices:
        st.info("Nothing to do: no rows need filling (or outputs already filled).")
else:
    st.info("Upload your Excel to begin.")
