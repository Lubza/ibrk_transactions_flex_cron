import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
from sqlalchemy import create_engine
import hashlib

flex_query_id = os.environ.get("FLEX_QUERY_ID")
token = os.environ.get("FLEX_TOKEN")
DB_URL = os.environ.get("DB_URL")
TABLE_NAME = "ib_transactions"

# -
# ---- Symbol normalization settings ----
UNDERLYING_SYMBOL_MAP = {
    "TUI1": "TUI1.DE",
    "VNA": "VNA.DE",
}
# --------------------------------------
#-

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

#-
# ---- Normalize UnderlyingSymbol using mapping ----
if not df.empty and "UnderlyingSymbol" in df.columns:
    u_raw = df["UnderlyingSymbol"]

    # normalize input
    u_norm = u_raw.astype(str).str.strip().str.upper()

    # apply mapping only where key exists
    df["UnderlyingSymbol"] = u_norm.map(UNDERLYING_SYMBOL_MAP).fillna(u_norm)

    changed = (u_norm != df["UnderlyingSymbol"]).sum()
    if changed > 0:
        print(f"Normalized UnderlyingSymbol using mapping: {changed} rows updated.")
# -------------------------------------------------
#-

# ---- Create Trade_id (hash) ----
if not df.empty:
    # stable formatting for Quantity to avoid "20" vs "20.0"
    def fmt_qty(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        # remove trailing .0 if it's an integer
        if float(x).is_integer():
            return str(int(x))
        # otherwise keep reasonable precision
        return f"{float(x):.6f}".rstrip("0").rstrip(".")

    def make_trade_id(row):
        parts = [
            str(row.get("Symbol") or "").strip().upper(),
            str(row.get("AssetClass") or "").strip().upper(),
            str(row.get("Buy/Sell") or "").strip().upper(),
            str(row.get("CurrencyPrimary") or "").strip().upper(),
            str(row.get("TradeDate") or "").strip(),   # keep as provided (YYYYMMDD)
            fmt_qty(row.get("Quantity")),
        ]
        fingerprint = "|".join(parts)
        return hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()

    df["Trade_id"] = df.apply(make_trade_id, axis=1)

    # optional: quick sanity print
    print(f"Generated Trade_id for {df['Trade_id'].notna().sum()} rows.")
# --------------------------------


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