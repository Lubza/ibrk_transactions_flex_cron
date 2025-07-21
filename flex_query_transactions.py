import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
from sqlalchemy import create_engine

flex_query_id = os.environ.get("FLEX_QUERY_ID")
token = os.environ.get("FLEX_TOKEN")
DB_URL = os.environ.get("DB_URL")
TABLE_NAME = "ib_transactions"

# 1. Get reference code
url_req = f"https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?t={token}&q={flex_query_id}&v=3"
response = requests.get(url_req)
print(response.text)

if "<ReferenceCode>" not in response.text:
    print("ReferenceCode was not found in the response. Probably there is a problem with the token, query, or no data is available.")
    exit()

reference_code = response.text.split("<ReferenceCode>")[1].split("</ReferenceCode>")[0]

# 2. Wait shortly until the report is generated
time.sleep(3)

# 3. Download XML report
url_report = f"https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement?t={token}&q={reference_code}&v=3"
result = requests.get(url_report)
xml_string = result.content.decode("utf-8")

# 4. Parse XML into DataFrame
root = ET.fromstring(xml_string)
trades = []
for tr in root.findall(".//Trade"):
    trades.append({
        "Symbol": tr.get("symbol"),
        "UnderlyingSymbol": tr.get("underlyingSymbol"),
        "Description": tr.get("description"),
        "AssetClass": tr.get("assetCategory"),
        "Put/Call": tr.get("putCall"),
        "Buy/Sell": tr.get("buySell"),
        "CurrencyPrimary": tr.get("currency"),
        "Expiry": tr.get("expiry"),
        "TradeDate": tr.get("tradeDate"),
        "TradePrice": float(tr.get("tradePrice")) if tr.get("tradePrice") else None,
        "ClosePrice": float(tr.get("closePrice")) if tr.get("closePrice") else None,
        "IBCommissionCurrency": tr.get("ibCommissionCurrency"),
        "FXRateToBase": float(tr.get("fxRateToBase")) if tr.get("fxRateToBase") else None,
        "Quantity": float(tr.get("quantity")) if tr.get("quantity") else None,
        "Proceeds": float(tr.get("proceeds")) if tr.get("proceeds") else None,
        "IBCommission": float(tr.get("ibCommission")) if tr.get("ibCommission") else None,
        "NetCash": float(tr.get("netCash")) if tr.get("netCash") else None,
        "Strike": tr.get("strike"),
        "Note": tr.get("note")
    })
df = pd.DataFrame(trades)

if not df.empty:
    print("Read trades:")
    print(df)

    # Save to database: append/merge new records
    try:
        engine = create_engine(DB_URL)
        try:
            df_old = pd.read_sql(TABLE_NAME, engine)
        except Exception:
            df_old = pd.DataFrame()
        ALL_COLUMNS = list(df.columns)
        common = [col for col in ALL_COLUMNS if col in df.columns and col in df_old.columns]
        df_merged = pd.concat([df_old, df], ignore_index=True)
        df_merged = df_merged.drop_duplicates(subset=common)
        df_merged.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
        print(f"Successfully saved {len(df_merged)} unique records to table '{TABLE_NAME}' in the database.")
    except Exception as e:
        print("\nError while saving to database:")
        print(str(e))
else:
    print("No new trades found, nothing is saved to the database.")