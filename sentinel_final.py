import yfinance as yf
import pandas as pd
import requests
import time
from datetime import datetime, timedelta

ASSETS = {
    "DE000ENER610": {"ticker": "ENR.DE", "name": "Siemens Energy"},
    "DE000BASF111": {"ticker": "BAS.DE", "name": "BASF"},
    "DE000SAPG003": {"ticker": "SAP.DE", "name": "SAP"},
    "DE0005190003": {"ticker": "BMW.DE", "name": "BMW"}
}

def get_realtime_price(isin):
    # Die hochpräzise Quelle (Tradegate/LS Exchange) für den Abgleich
    url = f"https://www.tradegate.de/refresh.php?isin={isin}"
    try:
        res = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        parts = res.text.split('|')
        if len(parts) > 2:
            return float(parts[2].replace(',', '.'))
    except:
        return None
    return None

def fetch_hybrid_data(isin, info):
    print(f"📡 Kalibrierung {info['name']}...")
    ticker = info['ticker']
    
    # 1. Yahoo-Historie (Das Rückgrat)
    stock = yf.Ticker(ticker)
    df = stock.history(period="max", interval="1d")
    
    # 2. Realtime-Stand (Die aktuelle Wahrheit)
    rt_price = get_realtime_price(isin)
    
    # 3. Schnittstellen-Abgleich (Sicherstellen, dass Yahoo nicht hinterherhinkt)
    # Wir nehmen die letzten 7 Tage von Yahoo und prüfen auf Konsistenz
    data_list = []
    for ts, row in df.iterrows():
        data_list.append({
            'Timestamp': ts.strftime('%Y-%m-%d'),
            'ID': ticker,
            'Asset': info['name'],
            'Price': round(row['Close'], 2),
            'Source': "YAHOO_HIST"
        })

    # 4. Die "Goldene Brücke": Heute mit Realtime-Preis überschreiben/ergänzen
    today_str = datetime.now().strftime('%Y-%m-%d')
    if rt_price:
        # Falls Yahoo für heute schon einen (verzögerten) Wert hat -> Überschreiben
        # Falls nicht -> Anhängen
        found = False
        for entry in data_list:
            if entry['Timestamp'] == today_str:
                entry['Price'] = rt_price
                entry['Source'] = "RT_CALIBRATED"
                found = True
        
        if not found:
            data_list.append({
                'Timestamp': today_str,
                'ID': ticker,
                'Asset': info['name'],
                'Price': rt_price,
                'Source': "RT_CALIBRATED"
            })
            
    return data_list

if __name__ == "__main__":
    final_records = []
    for isin, info in ASSETS.items():
        records = fetch_hybrid_data(isin, info)
        final_records.extend(records)
        time.sleep(1)

    # In CSV sichern
    df_final = pd.DataFrame(final_records)
    df_final.to_csv('sentinel_history.csv', index=False)
    
    # Verifizierung der Schnittstelle im Log
    print("\n--- 🛡️ SCHNITTSTELLEN-CHECK ---")
    for isin, info in ASSETS.items():
        last_entry = [r for r in final_records if r['ID'] == info['ticker']][-1]
        print(f"✅ {info['name']}: Stand {last_entry['Timestamp']} -> {last_entry['Price']} € ({last_entry['Source']})")
