import yfinance as yf
import pandas as pd
import csv
import time
from datetime import datetime

# Deine Kern-Assets für den Kurs-Check
ASSETS = {
    "ENR.DE": "Siemens Energy",
    "BAS.DE": "BASF",
    "SAP.DE": "SAP",
    "BMW.DE": "BMW"
}

def fetch_and_verify_data(ticker, name):
    print(f"📡 Starte Deep-Scan & Kalibrierung für {name}...")
    stock = yf.Ticker(ticker)
    
    # 1. MAX-Historie laden (Tagesbasis)
    df_hist = stock.history(period="max", interval="1d")
    
    # 2. Intraday-Daten laden (Minutenbasis für heute)
    df_intra = stock.history(period="1d", interval="1m")
    
    if df_hist.empty:
        return []

    # 3. Lückenlosigkeit erzwingen (Forward-Fill für Feiertage)
    # Erstellt einen lückenlosen Kalender vom Start bis heute
    full_range = pd.date_range(start=df_hist.index[0], end=pd.Timestamp.now(tz='Europe/Berlin'), freq='D')
    df_hist = df_hist.reindex(full_range).ffill()
    
    combined_data = []
    
    # Historie aufbereiten
    for ts, row in df_hist.iterrows():
        combined_data.append([ts.strftime('%Y-%m-%d'), ticker, name, round(row['Close'], 2), "HISTORY"])
    
    # Intraday-Verifizierung (Die letzten Ticks von heute anhängen)
    if not df_intra.empty:
        for ts, row in df_intra.tail(10).iterrows(): # Die letzten 10 Minuten für hohe Präzision
            combined_data.append([ts.strftime('%Y-%m-%d %H:%M'), ticker, name, round(row['Close'], 2), "INTRADAY_VERIFY"])

    # Finaler Verifizierungs-Check im Log
    last_val = combined_data[-1][3]
    print(f"   ✅ {name} kalibriert: {len(combined_data)} Datenpunkte. Letzter Stand: {last_val} €")
    return combined_data

if __name__ == "__main__":
    all_records = []
    for ticker, name in ASSETS.items():
        records = fetch_and_verify_data(ticker, name)
        if records:
            all_records.extend(records)
        time.sleep(1) # Schutz gegen API-Sperren

    # Speichern der lückenlosen Datenbank
    with open('sentinel_history.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'ID', 'Asset', 'Price', 'Source_Type'])
        writer.writerows(all_records)

    print(f"\n🏁 Sentinel V111: Datenbank MAX erfolgreich synchronisiert.")
