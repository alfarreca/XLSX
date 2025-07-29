import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

# Load your classification file
input_file = "Dual_Classification_Fully_Filled.xlsx"
df = pd.read_excel(input_file)

# Mapping from Finviz industries to your GICS categories
finviz_to_gics = {
    "Semiconductors": ("Information Technology", "Semiconductors", "Semiconductors & Semiconductor Equipment", "Semiconductors"),
    "Software—Application": ("Information Technology", "Software & Services", "Software", "Application Software"),
    "Software—Infrastructure": ("Information Technology", "Software & Services", "Software", "Systems Software"),
    "Auto Manufacturers": ("Consumer Discretionary", "Automobiles & Components", "Automobiles", "Automobile Manufacturers"),
    "Biotechnology": ("Health Care", "Pharmaceuticals, Biotechnology & Life Sciences", "Biotechnology", "Biotechnology"),
    "Information Technology Services": ("Information Technology", "Software & Services", "IT Services", "IT Consulting & Other Services"),
    "Communication Equipment": ("Information Technology", "Technology Hardware & Equipment", "Communications Equipment", "Communications Equipment"),
    # Add more mappings as needed
}

# Finviz scrape function
def get_finviz_industry(symbol):
    try:
        url = f"https://finviz.com/quote.ashx?t={symbol}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        table = soup.find_all('table', class_='snapshot-table2')[0]
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            for i in range(len(cells)):
                if cells[i].text.strip() == "Industry":
                    return cells[i+1].text.strip()
    except:
        return None

# Apply Finviz mapping to each row with missing Industry
for i, row in df.iterrows():
    symbol = row['Symbol']
    if '.' in str(symbol):  # skip non-US tickers
        continue
    if pd.isna(row['Industry']) or pd.isna(row['Sub-Industry']):
        print(f"Fetching Finviz industry for: {symbol}")
        finviz_ind = get_finviz_industry(symbol)
        if finviz_ind and finviz_ind in finviz_to_gics:
            sector, ind_group, industry, sub_industry = finviz_to_gics[finviz_ind]
            if pd.isna(row['Sector']):
                df.at[i, 'Sector'] = sector
            if pd.isna(row['Industry Group']):
                df.at[i, 'Industry Group'] = ind_group
            if pd.isna(row['Industry']):
                df.at[i, 'Industry'] = industry
            if pd.isna(row['Sub-Industry']):
                df.at[i, 'Sub-Industry'] = sub_industry
        time.sleep(1)  # respectful delay

# Save the updated file
output_file = "Dual_Classification_GICS_Completed.xlsx"
df.to_excel(output_file, index=False)
print(f"\n✅ Done! File saved as: {output_file}")
