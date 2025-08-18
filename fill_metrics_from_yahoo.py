
import pandas as pd
import numpy as np
import yfinance as yf

IN_PATH = "nuclear_watchlist_metrics_template.xlsx"
OUT_PATH = "nuclear_watchlist_metrics_filled.xlsx"

def _fast_info_dict(t: yf.Ticker) -> dict:
    try:
        fi = getattr(t, "fast_info", None)
        if fi is None:
            return {}
        return dict(fi.items()) if hasattr(fi, "items") else {
            k: getattr(fi, k) for k in (
                "last_price","lastPrice","last_close","market_cap","marketCap",
                "currency","shares_outstanding","regular_market_previous_close"
            ) if getattr(fi, k, None) is not None
        }
    except Exception:
        return {}

def _get_price_mcap_shares(t: yf.Ticker, info: dict, fi: dict):
    price = (
        fi.get("last_price") or fi.get("lastPrice") or fi.get("last_close") or
        fi.get("regular_market_previous_close") or info.get("currentPrice") or np.nan
    )
    shares = fi.get("shares_outstanding") or info.get("sharesOutstanding") or np.nan
    mcap = fi.get("market_cap") or fi.get("marketCap") or info.get("marketCap") or np.nan
    if (pd.isna(mcap) or not mcap) and not pd.isna(price) and not pd.isna(shares) and price and shares:
        mcap = float(price) * float(shares)
    if pd.isna(price) or price == 0:
        try:
            h = t.history(period="5d", auto_adjust=False)
            if not h.empty:
                price = float(h["Close"].iloc[-1])
        except Exception:
            pass
    return price, mcap, shares

def _safe_div_yield(info: dict, price: float) -> float:
    try:
        rate = info.get("trailingAnnualDividendRate")
        if rate is not None and price and price > 0:
            return float(rate) / float(price) * 100.0
    except Exception:
        pass
    y = info.get("dividendYield", None)
    if y is None:
        return 0.0
    try:
        y = float(y)
    except Exception:
        return 0.0
    return y if y > 1.0 else y * 100.0

def _quarterly_sum(df: pd.DataFrame, row_candidates, n=4):
    try:
        if isinstance(df, pd.DataFrame) and not df.empty:
            for r in row_candidates:
                if r in df.index:
                    s = pd.to_numeric(df.loc[r].dropna(), errors="coerce").sort_index(ascending=False)
                    vals = s.values[:n]
                    if len(vals):
                        return float(np.nansum(vals))
    except Exception:
        pass
    return np.nan

def _latest_value(df: pd.DataFrame, row_candidates):
    try:
        if isinstance(df, pd.DataFrame) and not df.empty:
            for r in row_candidates:
                if r in df.index:
                    s = pd.to_numeric(df.loc[r].dropna(), errors="coerce").sort_index(ascending=False)
                    if len(s):
                        return float(s.iloc[0])
    except Exception:
        pass
    return np.nan

def fetch_row(symbol: str) -> dict:
    t = yf.Ticker(symbol)
    try:
        info = t.get_info() if hasattr(t, "get_info") else t.info
    except Exception:
        info = {}
    fi = _fast_info_dict(t)
    price, mcap, shares = _get_price_mcap_shares(t, info, fi)

    # Yahoo basics
    pe_y  = info.get("trailingPE", np.nan)
    ps_y  = info.get("priceToSalesTrailing12Months", np.nan)
    pb_y  = info.get("priceToBook", np.nan)
    pfcf_y = info.get("priceToFcf", np.nan)
    de_y  = info.get("debtToEquity", np.nan)

    # Derive missing pieces
    eps_ttm = info.get("trailingEps") or info.get("epsTrailingTwelveMonths") or np.nan
    if pd.isna(eps_ttm):
        try:
            eps_ttm = _quarterly_sum(t.quarterly_financials, ["Basic EPS", "Diluted EPS"], 4)
        except Exception:
            pass

    ttm_rev = np.nan
    try:
        ttm_rev = _quarterly_sum(t.quarterly_financials, ["Total Revenue","Revenue"], 4)
    except Exception:
        pass
    if pd.isna(ttm_rev):
        ttm_rev = info.get("totalRevenue", np.nan)

    bvps = info.get("bookValue", np.nan)
    total_equity = np.nan
    try:
        total_equity = _latest_value(t.quarterly_balance_sheet, ["Total Stockholder Equity","Total Equity"])
    except Exception:
        pass
    if pd.isna(total_equity) and not pd.isna(bvps) and not pd.isna(shares):
        total_equity = float(bvps) * float(shares)

    total_debt = np.nan
    try:
        total_debt = _latest_value(t.quarterly_balance_sheet, ["Total Debt"])
        if pd.isna(total_debt):
            sld = _latest_value(t.quarterly_balance_sheet, ["Short Long Term Debt"])
            ltd = _latest_value(t.quarterly_balance_sheet, ["Long Term Debt"])
            total_debt = (0 if pd.isna(sld) else sld) + (0 if pd.isna(ltd) else ltd)
    except Exception:
        pass
    if pd.isna(total_debt):
        total_debt = info.get("totalDebt", np.nan)

    ttm_fcf = info.get("freeCashflow", np.nan)
    if pd.isna(ttm_fcf):
        try:
            ttm_fcf = _quarterly_sum(t.quarterly_cashflow, ["Free Cash Flow"], 4)
            if pd.isna(ttm_fcf):
                cfo = _quarterly_sum(t.quarterly_cashflow, ["Total Cash From Operating Activities","Operating Cash Flow"], 4)
                capex = _quarterly_sum(t.quarterly_cashflow, ["Capital Expenditures"], 4)
                if not pd.isna(cfo) and not pd.isna(capex):
                    ttm_fcf = cfo - capex
        except Exception:
            pass

    # Ratios
    pe = pe_y if not pd.isna(pe_y) else (float(price)/float(eps_ttm) if (not pd.isna(price) and not pd.isna(eps_ttm) and eps_ttm) else np.nan)
    ps = ps_y if not pd.isna(ps_y) else (float(mcap)/float(ttm_rev) if (not pd.isna(mcap) and not pd.isna(ttm_rev) and ttm_rev) else np.nan)
    pb = pb_y if not pd.isna(pb_y) else (
        float(price)/float(bvps) if (not pd.isna(price) and not pd.isna(bvps) and bvps) else
        (float(mcap)/float(total_equity) if (not pd.isna(mcap) and not pd.isna(total_equity) and total_equity) else np.nan)
    )
    pfcf = pfcf_y if not pd.isna(pfcf_y) else (float(mcap)/float(ttm_fcf) if (not pd.isna(mcap) and not pd.isna(ttm_fcf) and ttm_fcf and ttm_fcf>0) else np.nan)
    de = de_y if not pd.isna(de_y) else (float(total_debt)/float(total_equity) if (not pd.isna(total_debt) and not pd.isna(total_equity) and total_equity) else np.nan)

    return {
        "YF_Symbol": symbol,
        "P/E": pe,
        "P/B": pb,
        "Div Yield (%)": _safe_div_yield(info, price),
        "P/S": ps,
        "P/FCF": pfcf,
        "Debt/Equity": de,
    }

def main():
    df = pd.read_excel(IN_PATH)
    df.columns = [str(c).strip() for c in df.columns]
    sym_col = [c for c in df.columns if c.lower() in ("symbol","yf_symbol","ticker","ticker_symbol")]
    sym_col = sym_col[0] if sym_col else df.columns[0]

    out_rows = []
    for sym in df[sym_col].astype(str).str.strip().str.upper().replace({"": None}).dropna():
        try:
            row = {"Symbol": sym}
            row.update(fetch_row(sym))
        except Exception as e:
            row = {"Symbol": sym, "YF_Symbol": sym, "P/E": np.nan, "P/B": np.nan, "Div Yield (%)": np.nan,
                   "P/S": np.nan, "P/FCF": np.nan, "Debt/Equity": np.nan}
        out_rows.append(row)

    out = pd.DataFrame(out_rows)
    with pd.ExcelWriter(OUT_PATH, engine="xlsxwriter") as writer:
        out.to_excel(writer, index=False, sheet_name="Metrics")
    print(f"Saved: {OUT_PATH}")

if __name__ == "__main__":
    main()
