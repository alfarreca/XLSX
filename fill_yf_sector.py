# fill_yf_sector.py
import os, re, json, time, math
import pandas as pd

# ---------- CONFIG ----------
INPUT_XLSX  = r"/mnt/data/Copy of Russell 3000.xlsx"  # change to your local path if needed
SHEET_NAME  = None  # None = first sheet; or set to a name like "Sheet1"
TICKER_COL  = "Symbol"  # change if your column is named differently
EXCHANGE_COL = "Exchange"  # optional; improves symbol mapping for US vs non-US
OUTPUT_XLSX = os.path.splitext(INPUT_XLSX)[0] + " - with YF Sectors.xlsx"
CACHE_JSON  = os.path.splitext(INPUT_XLSX)[0] + " - yf_sector_cache.json"
REQUEST_DELAY_SEC = 0.6  # be nice to Yahoo; increase if you hit rate limits
# ----------------------------

# yfinance import
try:
    import yfinance as yf
except ImportError:
    raise SystemExit("Please install yfinance first: pip install yfinance>=0.2.40")

# yfinance moved .info → .get_info() in recent versions; we’ll prefer .get_info()
def safe_get_info(t: "yf.Ticker") -> dict:
    """Robustly fetch metadata dict from yfinance."""
    # Try get_info (new API). Fallback to .info if present.
    d = {}
    try:
        d = t.get_info()
        if isinstance(d, dict) and d:
            return d
    except Exception:
        pass
    try:
        d = getattr(t, "info", {})
    except Exception:
        d = {}
    return d if isinstance(d, dict) else {}

# Simple heuristic: convert US class-share dot tickers (e.g., BRK.B) to Yahoo’s dash form (BRK-B)
# but leave true exchange suffixes (e.g., .TO, .L, .DE) intact.
KNOWN_EX_SUFFIXES = {".TO",".V",".CN",".AX",".L",".SW",".PA",".BR",".BE",".DE",".F",".HM",".MU",".SG",
                     ".ST",".HE",".MI",".AS",".MC",".WA",".VI",".IR",".IS",".HK",".KS",".KQ",".T",".TW"}

CLASS_SHARE_DOT_PATTERN = re.compile(r"^[A-Z]+\.([A-Z]|[A-Z]\d)$")  # e.g., BRK.B or ABC.A1

def map_to_yahoo_symbol(symbol: str, exchange: str | None) -> str:
    sym = str(symbol).strip().upper()
    if not sym:
        return sym

    # If symbol already contains a known exchange suffix that’s not the US class-share pattern, leave it.
    for suf in KNOWN_EX_SUFFIXES:
        if sym.endswith(suf):
            return sym  # keep .TO/.L/etc.

    # If we have an Exchange column and it looks like US, apply class-share mapping
    is_us = False
    if exchange:
        ex = str(exchange).strip().upper()
        # Common US flags
        if any(k in ex for k in ("NYSE", "NASDAQ", "NSDQ", "OTC", "ARCA", "BATS", "AMEX", "NYSEMKT", "NMS")):
            is_us = True

    # If Exchange not provided, guess: if it has a dot and matches class-share pattern, treat as US
    if not exchange and CLASS_SHARE_DOT_PATTERN.match(sym):
        is_us = True

    if is_us and "." in sym:
        # Convert BRK.B → BRK-B; keep anything else intact
        base, tail = sym.split(".", 1)
        # Only convert if it looks like a share class (single letter or letter+digit)
        if re.fullmatch(r"[A-Z]\d?|[A-Z]", tail):
            return f"{base}-{tail}"
    return sym

def load_cache(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(path: str, data: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def fetch_sector_industry(yf_symbol: str) -> tuple[str | None, str | None]:
    """Return (sector, industry) from Yahoo for a given symbol."""
    t = yf.Ticker(yf_symbol)
    info = safe_get_info(t)
    if not info:
        return (None, None)
    sector = info.get("sector") or info.get("longBusinessSummary", None)  # fallback not ideal
    industry = info.get("industry") or info.get("industryKey") or info.get("industryDisp")
    # Normalize empty strings to None
    sector = sector if (sector and str(sector).strip()) else None
    industry = industry if (industry and str(industry).strip()) else None
    return (sector, industry)

def main():
    if not os.path.exists(INPUT_XLSX):
        raise SystemExit(f"Input file not found:\n{INPUT_XLSX}")

    df = pd.read_excel(INPUT_XLSX, sheet_name=SHEET_NAME)
    if TICKER_COL not in df.columns:
        raise SystemExit(f"Column '{TICKER_COL}' not found. Available columns: {list(df.columns)}")

    if "Sector (YF)" not in df.columns:
        df["Sector (YF)"] = pd.Series([None] * len(df))
    if "Industry (YF)" not in df.columns:
        df["Industry (YF)"] = pd.Series([None] * len(df))

    cache = load_cache(CACHE_JSON)

    # Build worklist
    total = len(df)
    unique_pairs = {}  # original index -> yf_symbol
    for i, row in df.iterrows():
        sym = str(row[TICKER_COL]).strip()
        if not sym or sym.lower() == "nan":
            continue
        exch = str(row[EXCHANGE_COL]).strip() if EXCHANGE_COL in df.columns else None
        yf_sym = map_to_yahoo_symbol(sym, exch)
        unique_pairs[i] = yf_sym

    # Fetch loop
    done = 0
    for idx, yf_sym in unique_pairs.items():
        # Skip if already present in dataframe (non-empty) or cached
        cur_sector = df.at[idx, "Sector (YF)"]
        cur_industry = df.at[idx, "Industry (YF)"]
        if (isinstance(cur_sector, str) and cur_sector.strip()) and (isinstance(cur_industry, str) and cur_industry.strip()):
            done += 1
            continue

        cache_key = yf_sym
        if cache_key in cache and isinstance(cache[cache_key], dict):
            res = cache[cache_key]
            df.at[idx, "Sector (YF)"] = res.get("sector")
            df.at[idx, "Industry (YF)"] = res.get("industry")
            done += 1
            continue

        # Fetch from Yahoo
        try:
            sector, industry = fetch_sector_industry(yf_sym)
        except Exception as e:
            sector, industry = (None, None)

        # Save into df and cache
        df.at[idx, "Sector (YF)"] = sector
        df.at[idx, "Industry (YF)"] = industry
        cache[cache_key] = {"sector": sector, "industry": industry}

        done += 1
        # progress & throttling
        if REQUEST_DELAY_SEC and REQUEST_DELAY_SEC > 0:
            time.sleep(REQUEST_DELAY_SEC)
        if done % 50 == 0:
            print(f"Processed {done}/{total}… saving cache.")
            save_cache(CACHE_JSON, cache)

    # Final save
    save_cache(CACHE_JSON, cache)

    # Keep your original columns order; append YF columns at the end (or move them next to Symbol)
    # If you prefer them next to the ticker, uncomment the block below:
    # cols = list(df.columns)
    # for col in ["Sector (YF)", "Industry (YF)"]:
    #     cols.remove(col)
    # insert_at = max(0, cols.index(TICKER_COL) + 1) if TICKER_COL in cols else len(cols)
    # cols[insert_at:insert_at] = ["Sector (YF)", "Industry (YF)"]
    # df = df[cols]

    # Write output
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)

    print(f"Done. Wrote: {OUTPUT_XLSX}")
    print(f"Cache: {CACHE_JSON}")

if __name__ == "__main__":
    main()
