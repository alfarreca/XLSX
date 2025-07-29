import pandas as pd
import yfinance as yf
import time

# Load your file
input_file = "Dual_Classification_GICS_Completed.xlsx"
df = pd.read_excel(input_file)

# Fill rules from Yahoo -> GICS
def map_yahoo_to_gics(yahoo_sector, yahoo_industry):
    sector = yahoo_sector
    ind_group = yahoo_sector  # Use sector again as fallback
    industry = yahoo_industry
    sub_industry = yahoo_industry
    return sector, ind_group, industry, sub_industry

# Process each row with missing data
for i, row in df.iterrows():
    symbol = row['Symbol']
    if pd.notna(row['Sector']) and pd.notna(row['Industry']) and pd.notna(row['Sub-Industry']):
        continue  # Skip already filled rows
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
        time.sleep(0.5)  # Be polite to Yahoo servers
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        continue

# Save output
output_file = "Dual_Classification_GICS_Yahoo_Filled.xlsx"
df.to_excel(output_file, index=False)
print(f"\n✅ Done! Saved to: {output_file}")
