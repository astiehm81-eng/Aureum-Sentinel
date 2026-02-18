import yfinance as yf
import pandas as pd
import csv
import time

ASSETS = {
    "ENR.DE": "Siemens Energy",
    "BAS.DE": "BASF",
    "SAP.DE": "SAP",
    "BMW.DE": "BMW"
}

def fetch_and_calibrate_max(ticker, name):
    print(f"📡 Deep-Scan & Kalibrierung (MAX) für {name}...")
    stock = yf.Ticker(ticker)
    
    # 1. Gesamte Historie laden
    df = stock.history(period="max", interval="1d")
    if df.empty:
        return []

    # 2. Lückenlosigkeit sicherstellen (Wochenenden/Feiertage füllen)
    # Wir erstellen einen lückenlosen Zeitindex
    full_index = pd.date_range(start=df.index[0], end=df.index[-1], freq='D')
    df = df.reindex(full_index)
    df['Close'] = df['Close'].ffill() # Letzten Kurs bei Lücken übernehmen
    
    # 3. Intraday-Abgleich (Letzte 24h in Minuten-Auflösung für den 'Last Stand')
    intra = stock.history(period="1d", interval="1m")
    
    data_output = []
    # Historische Daten (Tage)
    for ts, row in df.iterrows():
        data_output.append([ts.strftime('%Y-%m-%d'), ticker, name, round(row['Close'], 2), "HIST_DAILY"])
    
    # Intraday-Daten (Minuten von heute für die Echtzeit-Verifizierung)
    if not intra.empty:
        for ts, row in intra.iterrows():
            data_output.append([ts.strftime('%Y-%m-%d %H:%M'), ticker, name, round(row['Close'], 2), "INTRA_TICK"])

    # Verifizierung im Log
    last_price = data_output[-1][3]
    print(f"   ✅ {name} kalibriert. {len(data_output)} Punkte. Aktuell: {last_price} €")
    return data_output

if __name__ == "__main__":
    final_db = []
    for ticker, name in ASSETS.items():
        data = fetch_and_calibrate_max(ticker, name)
        if data:
            final_db.extend(data)
    
    # Speichern der lückenlosen "Goldenen Quelle"
    with open('sentinel_history.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'ID', 'Asset', 'Price', 'Type'])
        writer.writerows(final_db)

    print("\n🏁 Sentinel V110: MAX-Datenbank lückenlos synchronisiert.")
