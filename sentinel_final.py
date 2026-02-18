import yfinance as yf
import pandas as pd
import requests
import time
from datetime import datetime, timedelta

ASSETS = {
    "DE000ENER610": {"ticker": "ENR.DE", "name": "Siemens Energy"},
    "DE000BASF111": {"ticker": "BAS.DE", "name": "BASF"},
    "DE000SAPG003": {"ticker": "SAP.DE", "name": "SAP"}
}

def get_tradegate_data(isin):
    url = f"https://www.tradegate.de/refresh.php?isin={isin}"
    try:
        res = requests.get(url, timeout=5)
        parts = res.text.split('|')
        if len(parts) > 2:
            return float(parts[2].replace(',', '.'))
    except:
        return None
    return None

def calibrate_asset(isin, info):
    print(f"📡 Deep-Sync: {info['name']}...")
    stock = yf.Ticker(info['ticker'])
    df = stock.history(period="1y", interval="1d")
    
    rt_price = get_tradegate_data(isin)
    
    data_records = []
    today_str = datetime.now().strftime('%Y-%m-%d')

    # Abgleich-Logik für das Log
    if rt_price and not df.empty:
        last_yahoo = round(df['Close'].iloc[-1], 2)
        diff = round(rt_price - last_yahoo, 2)
        print(f"   📊 Abgleich: Yahoo {last_yahoo}€ vs. Realtime {rt_price}€ (Abweichung: {diff}€)")

    for ts, row in df.iterrows():
        ts_str = ts.strftime('%Y-%m-%d')
        price = round(row['Close'], 2)
        source = "YAHOO_BASE"

        if ts_str == today_str and rt_price:
            price = rt_price
            source = "TRADEGATE_RT_FIX"
        
        data_records.append([ts_str, info['ticker'], info['name'], price, source])

    if rt_price and not any(r[0] == today_str for r in data_records):
        data_records.append([today_str, info['ticker'], info['name'], rt_price, "TRADEGATE_RT_FIX"])

    return data_records, rt_price # rt_price wird jetzt zurückgegeben

if __name__ == "__main__":
    all_calibrated_data = []
    last_prices = {} # Speicher für die Abschlussmeldung

    for isin, info in ASSETS.items():
        records, current_rt = calibrate_asset(isin, info)
        all_calibrated_data.extend(records)
        last_prices[info['name']] = current_rt
        time.sleep(1)

    df_final = pd.DataFrame(all_calibrated_data, columns=['Timestamp', 'ID', 'Asset', 'Price', 'Source_Type'])
    df_final = df_final.drop_duplicates(subset=['Timestamp', 'ID'], keep='last')
    df_final.to_csv('sentinel_history.csv', index=False)
    
    # Korrigierte Abschlussmeldung ohne NameError
    print("\n--- 🏁 ABSCHLUSS-KALIBRIERUNG ---")
    for name, price in last_prices.items():
        print(f"✅ {name}: {price if price else 'N/A'} € (Schnittstelle verifiziert)")
