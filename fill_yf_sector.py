import io
import re
import time
import json
import pandas as pd
import streamlit as st

# ---------- Config ----------
KNOWN_EX_SUFFIXES = {
    ".TO",".V",".CN",".AX",".L",".SW",".PA",".BR",".BE",".DE",".F",".HM",".MU",".SG",
    ".ST",".HE",".MI",".AS",".MC",".WA",".VI",".IR",".IS",".HK",".KS",".KQ",".T",".TW"
}
REQUEST_DELAY_SEC = 0.6  # be nice to Yahoo in shared environments
MAX_RETRIES = 2          # per ticker
# ----------------------------

st.set_page_config(page_title="Fill Yahoo Sectors", layout="wide")
st.title("📊 Fill Sector/Industry from Yahoo Finance")

with st.sidebar:
    st.header("Settings")
    ticker_col_name = st.text_input("Ticker column name", value="Symbol")
    exchange_col_name = st.text_input("Exchange column (optional)", value="Exchange")
    sector_col_name = st.text_input("Output column: Sector", value="Sector (YF)")
    industry_col_name = st.text_input("Output column: Industry", value="Industry (YF)")
    skip_filled = st.checkbox("Skip rows where both outputs are already filled", value=True)
    st.caption("Tip: Keep defaults unless your file uses different column names.")

uploaded = st.file_uploader("Upload your Excel file", type=["xlsx"])
sheet_name = None
if uploaded:
    try:
        xl = pd.ExcelFile(uploaded)
        if len(xl.sheet_names) > 1:
            sheet_name = st.selectbox("Choose a sheet", options=xl.sheet_names, index=0)
        else:
            sheet_name = 0
        df = xl.parse(sheet_name)
    except Exception as e:
        st.error(f"Could not read Excel: {e}")
        st.stop()

    # Basic checks
    if ticker_col_name not in df.columns:
        st.error(f"Column '{ticker_col_name}' not found. Available: {list(df.columns)}")
        st.stop()

    # Ensure output columns exist
    if sector_col_name not in df.columns:
        df[sector_col_name] = None
    if industry_col_name not in df.columns:
        df[industry_col_name] = None

    # Optional preview
    with st.expander("Preview uploaded data"):
        st.dataframe(df.head(20), use_container_width=True)

    # --------- yfinance setup (lazy import) ----------
    try:
        import yfinance as yf
    except Exception as e:
        st.error("`yfinance` is not installed. Add it to requirements.txt:\n\n`yfinance>=0.2.40`")
        st.stop()

    class_share_pat = re.compile(r"^[A-Z]+\.([A-Z]|[A-Z]\d)$")

    def map_to_yahoo_symbol(symbol: str, exchange: str | None) -> str:
        """Map class-share US tickers like BRK.B to BRK-B, preserve true exchange suffixes."""
        sym = str(symbol).strip().upper()
        if not sym:
            return sym
        # Keep known non-US suffixes
        for suf in KNOWN_EX_SUFFIXES:
            if sym.endswith(suf):
                return sym

        is_us = False
        if exchange:
            ex = str(exchange).strip().upper()
            if any(k in ex for k in ("NYSE", "NASDAQ", "NSDQ", "OTC", "ARCA", "BATS", "AMEX", "NYSEMKT", "NMS")):
                is_us = True
        if not exchange and class_share_pat.match(sym):
            is_us = True

        if is_us and "." in sym:
            base, tail = sym.split(".", 1)
            if re.fullmatch(r"[A-Z]\d?|[A-Z]", tail):
                return f"{base}-{tail}"
        return sym

    @st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
    def cached_fetch(yf_symbol: str) -> dict:
        """Cached fetch for a single ticker: returns {'sector':..., 'industry':...} (can be None)."""
        # Try new API first
        t = yf.Ticker(yf_symbol)
        info = {}
        try:
            info = t.get_info()
        except Exception:
            # fallback to old .info if still available
            try:
                info = getattr(t, "info", {}) or {}
            except Exception:
                info = {}

        sector = info.get("sector")
        industry = info.get("industry") or info.get("industryKey") or info.get("industryDisp")
        sector = sector if sector and str(sector).strip() else None
        industry = industry if industry and str(industry).strip() else None
        return {"sector": sector, "industry": industry}

    def fetch_with_retry(yf_symbol: str) -> dict:
        last_err = None
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                res = cached_fetch(yf_symbol)
                return res
            except Exception as e:
                last_err = e
                time.sleep(0.75 * attempt)
        return {"sector": None, "industry": None, "error": str(last_err) if last_err else "unknown"}

    # Build worklist
    work_indices = []
    for i, row in df.iterrows():
        sym = str(row.get(ticker_col_name, "")).strip()
        if not sym:
            continue
        if skip_filled:
            if (str(df.at[i, sector_col_name]).strip() not in ("", "None", "nan") and
                str(df.at[i, industry_col_name]).strip() not in ("", "None", "nan")):
                continue
        work_indices.append(i)

    st.write(f"Tickers to process: **{len(work_indices)}** (of {len(df)})")

    start = st.button("▶️ Start Filling from Yahoo")
    if start and work_indices:
        progress = st.progress(0)
        status = st.empty()
        results_area = st.empty()

        processed = 0
        errors = 0
        for k, idx in enumerate(work_indices, start=1):
            sym = str(df.at[idx, ticker_col_name]).strip()
            exch = str(df.at[idx, exchange_col_name]).strip() if exchange_col_name in df.columns else None
            yf_sym = map_to_yahoo_symbol(sym, exch)

            res = fetch_with_retry(yf_sym)
            sector = res.get("sector")
            industry = res.get("industry")

            if sector: df.at[idx, sector_col_name] = sector
            if industry: df.at[idx, industry_col_name] = industry
            if not sector and not industry:
                errors += 1

            processed += 1
            progress.progress(int(100 * processed / len(work_indices)))
            status.write(f"Processing {k}/{len(work_indices)} • {sym} → {yf_sym} • "
                         f"Sector='{sector}' Industry='{industry}'")

            # polite throttle
            time.sleep(REQUEST_DELAY_SEC)

            if k % 50 == 0:
                results_area.info(f"Checkpoint: processed {k} rows…")

        st.success(f"Done. Processed {processed} rows. Empty results: {errors}")

        # Show a small sample and enable download
        st.subheader("Sample of Updated Data")
        st.dataframe(df.head(50), use_container_width=True)

        # Prepare Excel download
        out_buf = io.BytesIO()
        with pd.ExcelWriter(out_buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        out_buf.seek(0)
        st.download_button(
            label="⬇️ Download updated Excel",
            data=out_buf,
            file_name="with_yf_sectors.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    elif start and not work_indices:
        st.info("Nothing to do: no rows need filling (or outputs already filled).")
else:
    st.info("Upload your Excel to begin.")
