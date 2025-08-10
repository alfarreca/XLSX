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
# ----------------------------

st.set_page_config(page_title="Fill Sector/Industry from Yahoo Finance", layout="wide")
st.title("📊 Fill Sector/Industry from Yahoo Finance")

with st.sidebar:
    st.header("Settings")
    ticker_col_name = st.text_input("Ticker column name", value="Symbol")
    exchange_col_name = st.text_input("Exchange column (optional)", value="Exchange")
    sector_col_name = st.text_input("Output column: Sector", value="Sector (YF)")
    industry_col_name = st.text_input("Output column: Industry", value="Industry (YF)")
    skip_filled = st.checkbox("Skip rows already filled (both columns)", value=True)
    request_delay = st.number_input("Delay per request (seconds)", value=0.7, step=0.1, min_value=0.0)
    max_retries = st.number_input("Max retries per ticker", value=1, step=1, min_value=0)
    checkpoint_every = st.number_input("Checkpoint save every N rows", value=50, step=10, min_value=10)
    do_exists_check = st.checkbox("Pre-check ticker exists (faster fail on dead tickers)", value=True)
    st.caption("Tip: Increase delay if you hit Yahoo rate limits.")

uploaded = st.file_uploader("Upload your Excel file", type=["xlsx"])
sheet_name = None

# Silence chatty logs from libs
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

    # yfinance (lazy import so the app page renders)
    try:
        import yfinance as yf
    except Exception:
        st.error("`yfinance` is not installed. Add to requirements.txt:\n\n`yfinance>=0.2.40`")
        st.stop()

    class_share_pat = re.compile(r"^[A-Z]+\.([A-Z0-9]{1,3})$")  # broader: e.g., BRK.B, XYZ.A, ABC.A1

    def map_to_yahoo_symbol(symbol: str, exchange: str | None) -> str:
        """
        Convert US class-share tickers with a dot to Yahoo's dash (BRK.B -> BRK-B).
        Preserve true exchange suffixes like .L, .TO, .HK, etc.
        """
        sym = str(symbol).strip().upper()
        if not sym:
            return sym
        for suf in KNOWN_EX_SUFFIXES:
            if sym.endswith(suf):
                return sym  # keep native exchange suffix as-is

        is_us = False
        if exchange:
            ex = str(exchange).strip().upper()
            if any(k in ex for k in ("NYSE", "NASDAQ", "NSDQ", "OTC", "ARCA", "BATS", "AMEX", "NYSEMKT", "NMS")):
                is_us = True
        if not exchange and class_share_pat.match(sym):
            is_us = True

        if is_us and "." in sym:
            base, tail = sym.split(".", 1)
            if re.fullmatch(r"[A-Z0-9]{1,3}", tail):
                return f"{base}-{tail}"
        return sym

    @st.cache_data(ttl=60*60*24, show_spinner=False)
    def cached_get_info(yf_symbol: str) -> dict:
        """
        Cached metadata fetch. Returns dict with optional keys sector/industry.
        Never raises – returns {} on failure.
        """
        try:
            t = yf.Ticker(yf_symbol)
            try:
                info = t.get_info()
            except Exception:
                # fallback to legacy .info
                info = getattr(t, "info", {}) or {}
            if not isinstance(info, dict):
                return {}
            return {
                "sector": (info.get("sector") or None),
                "industry": (info.get("industry") or info.get("industryKey") or info.get("industryDisp") or None),
            }
        except Exception:
            return {}

    @st.cache_data(ttl=60*60*24, show_spinner=False)
    def cached_exists(yf_symbol: str) -> bool:
        """
        Lightweight existence check. Uses fast_info as a proxy; if it blows up or
        has no market/last_price, assume not tradable.
        """
        try:
            t = yf.Ticker(yf_symbol)
            # some symbols return an empty dict – treat as non-existent
            fi = getattr(t, "fast_info", None)
            if not fi:
                # last resort: 1d history – may still 404 but is cached
                hist = t.history(period="1d")
                return not hist.empty
            # Many dead tickers have missing attributes entirely
            market = getattr(fi, "market", None)
            return market is not None
        except Exception:
            return False

    # Build worklist
    work_indices = []
    for i, row in df.iterrows():
        sym = str(row.get(ticker_col_name, "")).strip()
        if not sym:
            continue
        # skip if already filled
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

        # Error log rows we will turn into a DataFrame at the end
        err_rows = []

        for k, idx in enumerate(work_indices, start=1):
            sym = str(df.at[idx, ticker_col_name]).strip()
            exch = str(df.at[idx, exchange_col_name]).strip() if exchange_col_name in df.columns else None
            yf_sym = map_to_yahoo_symbol(sym, exch)

            # Optional quick existence check
            if do_exists_check and not cached_exists(yf_sym):
                failures += 1
                err_rows.append({"Symbol": sym, "Mapped": yf_sym, "Status": "not_found", "Sector": None, "Industry": None, "Error": "existence_check_failed"})
                status.write(f"❌ {k}/{len(work_indices)} • {sym} → {yf_sym} • not found (pre-check)")
                # still checkpoint and continue
                if k % checkpoint_every == 0:
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
                    # If we got at least something, break
                    if result.get("sector") or result.get("industry"):
                        break
                except Exception as e:
                    last_err_msg = str(e)
                time.sleep(0.4 * (attempt + 1))

            sector = result.get("sector")
            industry = result.get("industry")

            # Write back only explicit values (avoid FutureWarning)
            if isinstance(sector, str) and sector.strip():
                df.at[idx, sector_col_name] = sector.strip()
            if isinstance(industry, str) and industry.strip():
                df.at[idx, industry_col_name] = industry.strip()

            ok = bool((isinstance(sector, str) and sector.strip()) or (isinstance(industry, str) and industry.strip()))
            if not ok:
                failures += 1
                err_rows.append({"Symbol": sym, "Mapped": yf_sym, "Status": "empty", "Sector": sector, "Industry": industry, "Error": last_err_msg})

            processed += 1
            progress.progress(int(100 * processed / len(work_indices)))
            prefix = "✅" if ok else "⚠️"
            status.write(f"{prefix} {k}/{len(work_indices)} • {sym} → {yf_sym} • "
                         f"Sector='{sector}' Industry='{industry}'")

            # polite throttle
            time.sleep(request_delay)

            # Checkpoint
            if k % int(checkpoint_every) == 0:
                save_checkpoint(df)
                st.info(f"Checkpoint saved at {k} rows.")

        # Save final checkpoint
        save_checkpoint(df)
        st.success(f"Done. Processed {processed} rows. Empty/failed: {failures}")

        # Persist logs in session and present downloads
        if err_rows:
            logs_df = pd.DataFrame(err_rows)
            st.session_state.logs = err_rows
            st.subheader("Errors / Empty Results")
            st.dataframe(logs_df.head(100), use_container_width=True)

            # Download logs
            csv_buf = io.StringIO()
            logs_df.to_csv(csv_buf, index=False)
            st.download_button("⬇️ Download error log (CSV)", data=csv_buf.getvalue(),
                               file_name="yf_sector_errors.csv", mime="text/csv")

        # Show a sample and enable full download
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
