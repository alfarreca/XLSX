import pandas as pd
import yfinance as yf
import time

# Load your file
input_file = "Dual_Classification_GICS_Completed.xlsx"
df = pd.read_excel(input_file)

# === GICS FILLING ===
def map_yahoo_to_gics(yahoo_sector, yahoo_industry):
    sector = yahoo_sector
    ind_group = yahoo_sector
    industry = yahoo_industry
    sub_industry = yahoo_industry
    return sector, ind_group, industry, sub_industry

# === THEME TAGGING ===
def assign_themes(name, industry, sub_industry):
    themes = []

    name = name.lower()
    industry = str(industry).lower()
    sub_industry = str(sub_industry).lower()

    if any(tag in name for tag in ["robot", "ai", "automation", "vision"]):
        themes.append("AI")
        themes.append("Robotics")
    if "quantum" in name or "quantum" in sub_industry:
        themes.append("Quantum Computing")
    if "gold" in name or "gold" in industry:
        themes.append("Gold")
    if "copper" in name or "copper" in sub_industry:
        themes.append("Copper")
    if "uranium" in name:
        themes.append("Uranium")
    if "rare" in name or "rare earth" in sub_industry:
        themes.append("Rare Earths")
    if any(x in name for x in ["aero", "defense", "space", "satellite"]):
        themes.append("Defense")
        if "space" in name:
            themes.append("Space")
    if "semiconductor" in sub_industry:
        themes.append("Semiconductors")
    if "blockchain" in name or "crypto" in name or "bitcoin" in name:
        themes.append("Crypto")

    return ", ".join(sorted(set(themes))) if themes else None

# === FETCH GICS USING YAHOO FINANCE ===
for i, row in df.iterrows():
    symbol = row['Symbol']
    if pd.notna(row['Sector']) and pd.notna(row['Industry']) and pd.notna(row['Sub-Industry']):
        continue  # Skip fully classified rows
    try:
        print(f"Fetching Yahoo profile for: {symbol}")
        ticker = yf.Ticker(symbol)
        info = ticker.info
        yahoo_sector = info.get("sector")
        yahoo_industry = info.get("industry")
        if yahoo_sector and yahoo_industry:
            sector, ind_group, industry, sub_industry = map_yahoo_to_gics(yahoo_sector, yahoo_industry)
            if pd.isna(row['Sector']):
                df.at[i, 'Sector'] = sector
            if pd.isna(row['Industry Group']):
                df.at[i, 'Industry Group'] = ind_group
            if pd.isna(row['Industry']):
                df.at[i, 'Industry'] = industry
            if pd.isna(row['Sub-Industry']):
                df.at[i, 'Sub-Industry'] = sub_industry
        time.sleep(0.5)
    except Exception as e:
        print(f"⚠️ Error fetching {symbol}: {e}")
        continue

# === APPLY THEMES ===
name_col = "Name"
if "Name" not in df.columns:
    name_col = "Name_x" if "Name_x" in df.columns else "Name_y"

df["Theme(s)"] = df.apply(
    lambda row: assign_themes(str(row[name_col]), str(row["Industry"]), str(row["Sub-Industry"])),
    axis=1
)

# === SAVE RESULT ===
output_file = "Dual_Classification_GICS_Yahoo_Themed.xlsx"
df.to_excel(output_file, index=False)
print(f"\n✅ GICS + Themes saved to: {output_file}")
