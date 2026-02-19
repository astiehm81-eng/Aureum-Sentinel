import pandas as pd
import yfinance as yf
import requests
import io
import os
from datetime import datetime

FILENAME = "sentinel_master_storage.csv"

def get_stooq_history(symbol):
    """Holt die maximale historische Kurve von Stooq"""
    print(f"📡 Lade Stooq-Historie für {symbol}...")
    try:
        # Stooq Export-URL für maximale Historie (d = daily)
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=15)
        df = pd.read_csv(io.StringIO(res.text))
        
        if not df.empty:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            # Wir brauchen nur den Schlusskurs
            return df[['Close']].rename(columns={'Close': 'Price'})
    except Exception as e:
        print(f"⚠️ Stooq-Fehler: {e}")
    return pd.DataFrame()

def run_merge_and_sync():
    assets = {
        "DE0007164600": {"symbol": "SAP.DE", "name": "SAP SE"},
        "DE000ENER6Y0": {"symbol": "ENR.DE", "name": "Siemens Energy"}
    }
    
    all_data = []

    for isin, info in assets.items():
        print(f"\n🔄 Bearbeite {info['name']}...")
        
        # 1. Historisches Fundament von Stooq (Jahre zurück)
        df_stooq = get_stooq_history(info['symbol'])
        if not df_stooq.empty:
            df_stooq['Source'] = 'Stooq_Hist'
            df_stooq['ISIN'] = isin
        
        # 2. Präzisions-Update von Yahoo (letzte Tage & heute)
        print(f"📡 Hole Yahoo-Präzisionsdaten für {info['symbol']}...")
        ticker = yf.Ticker(info['symbol'])
        # Wir holen die letzten 60 Tage in 1h oder 1d Auflösung
        df_yahoo = ticker.history(period="60d", interval="1h")[['Close']].rename(columns={'Close': 'Price'})
        df_yahoo.index = df_yahoo.index.tz_localize(None)
        df_yahoo['Source'] = 'Yahoo_Live'
        df_yahoo['ISIN'] = isin

        # 3. MERGE: Yahoo überschreibt Stooq in der Überlappungszeit
        # Wir nehmen Stooq bis zum Startdatum von Yahoo, dann Yahoo
        combined = pd.concat([df_stooq[df_stooq.index < df_yahoo.index.min()], df_yahoo])
        all_data.append(combined)

    # 4. Master-Datei erstellen
    master_df = pd.concat(all_data)
    master_df.to_csv(FILENAME)
    
    print("\n" + "="*40)
    print(f"✅ MERGE ABGESCHLOSSEN")
    print(f"Datei: {FILENAME}")
    print(f"Einträge gesamt: {len(master_df)}")
    print(f"Zeitraum: {master_df.index.min()} bis {master_df.index.max()}")
    print("="*40)

if __name__ == "__main__":
    run_merge_and_sync()
