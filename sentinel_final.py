import os
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime
import io

FILENAME = "sentinel_master_storage.csv"

def get_stooq_data(symbol):
    """Holt aktuelle Daten von Stooq (CSV-Schnittstelle)"""
    try:
        url = f"https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        # Nutzen von io.StringIO für sauberes Einlesen des CSV-Streams
        df_stooq = pd.read_csv(io.StringIO(res.text))
        if not df_stooq.empty:
            return float(df_stooq['Close'].iloc[0])
    except Exception as e:
        print(f"Stooq-Fehler bei {symbol}: {e}")
    return None

def run_sentinel():
    # Assets für den Testlauf
    assets = {
        "DE0007164600": {"symbol": "SAP.DE", "name": "SAP SE"},
        "DE000ENER6Y0": {"symbol": "ENR.DE", "name": "Siemens Energy"}
    }
    
    print(f"🛡️ Sentinel CLEAN SLATE MODE (Testphase)")
    
    # RADIKAL-LÖSCHUNG: Bestehende CSV wird ignoriert/überschrieben
    print("🧹 Altdaten werden entfernt... Erzeuge frische Struktur.")
    master_df = pd.DataFrame(columns=['Price', 'Source', 'ISIN'])

    print(f"\n{'Asset':<18} | {'Yahoo (Anker)':<15} | {'Stooq (Live)':<12} | {'Status'}")
    print("-" * 75)

    for isin, info in assets.items():
        # 1. Yahoo für das Fundament
        try:
            ticker = yf.Ticker(info['symbol'])
            y_price = ticker.history(period="1d")['Close'].iloc[-1]
        except: y_price = None
        
        # 2. Stooq für die Präzision
        stooq_price = get_stooq_data(info['symbol'])
        
        if stooq_price:
            # Da wir alles gelöscht haben, wird dies der erste Eintrag (Anker)
            new_row = pd.DataFrame([{
                'Price': stooq_price, 
                'Source': 'Stooq_Initial', 
                'ISIN': isin
            }], index=[pd.Timestamp.now()])
            master_df = pd.concat([master_df, new_row])
            status = "✅ ERST-ANKER"
        else:
            status = "❌ OFFLINE"

        y_str = f"{y_price:.2f} €" if y_price else "N/A"
        s_str = f"{stooq_price:.2f} €" if stooq_price else "N/A"
        print(f"{info['name']:<18} | {y_str:>15} | {s_str:>12} | {status}")

    # Speichern der neuen, sauberen Datei
    master_df.to_csv(FILENAME)
    print("-" * 75)
    print(f"🚀 Neue Basis erstellt. Datei: {FILENAME} | Einträge: {len(master_df)}")

if __name__ == "__main__":
    run_sentinel()
