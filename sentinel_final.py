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

def calibrate_and_sync(ticker, name):
    print(f"🔍 Kalibrierung läuft für {name}...")
    stock = yf.Ticker(ticker)
    
    # 1. Historie holen (1 Monat zur Kalibrierung)
    hist = stock.history(period="1mo", interval="1d")
    
    # 2. Intraday holen (Heute)
    intra = stock.history(period="1d", interval="1m")
    
    if hist.empty or intra.empty:
        return None

    # Prüfung: Passt der gestrige Schlusskurs zum heutigen Start?
    yesterday_close = hist['Close'].iloc[-1]
    today_open = intra['Open'].iloc[0]
    drift = abs(yesterday_close - today_open)
    drift_pct = (drift / yesterday_close) * 100

    # Anzeige der Kalibrierung im Log
    print(f"   📊 Check: Gestern {yesterday_close:.2f} | Heute Open {today_open:.2f}")
    print(f"   ⚖️ Drift: {drift_pct:.4f}%")

    if drift_pct > 1.5:
        print(f"   ⚠️ Warnung: Hoher Drift bei {name}! Möglicher Gap.")
    else:
        print(f"   ✅ {name} ist kalibriert.")

    # Daten zusammenführen (History + Heute Intraday)
    combined = []
    # Historie (Tagesbasis)
    for ts, row in hist.iterrows():
        combined.append([ts.strftime('%Y-%m-%d'), ticker, name, round(row['Close'], 2), "HIST"])
    
    # Intraday (Minutenbasis - Letzte 60 Min zur Verifizierung)
    for ts, row in intra.tail(60).iterrows():
        combined.append([ts.strftime('%Y-%m-%d %H:%M'), ticker, name, round(row['Close'], 2), "INTRA"])
        
    return combined

if __name__ == "__main__":
    full_db = []
    for ticker, name in ASSETS.items():
        data = calibrate_and_sync(ticker, name)
        if data:
            full_db.extend(data)
    
    with open('sentinel_history.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'ID', 'Asset', 'Price', 'Type'])
        writer.writerows(full_db)

    print("\n🏁 Kalibrierung abgeschlossen. Daten sind synchron.")
