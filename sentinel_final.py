import os
import pandas as pd
import yfinance as yf
import requests
import re
from datetime import datetime

# DATEINAME SYNCHRONISIERT
FILENAME = "sentinel_master_storage.csv"

def get_tradegate_live(isin):
    try:
        url = f"https://www.tradegate.de/aktien.php?isin={isin}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        match = re.search(r'id="ask"[^>]*>([\d\.,]+)</span>', res.text)
        return float(match.group(1).replace('.', '').replace(',', '.')) if match else None
    except: return None

def run():
    isin, symbol = "DE000ENER6Y0", "ENR.DE"
    print(f"🛡️ Start Sentinel für {isin}")
    
    # 1. Daten laden oder Yahoo-Basis holen
    if os.path.exists(FILENAME):
        df = pd.read_csv(FILENAME, index_col=0)
        df.index = pd.to_datetime(df.index)
    else:
        df = yf.Ticker(symbol).history(period="max")[['Close']]
        df.columns = ['Price']
        df['Source'] = 'Yahoo_Legacy'
        df.index = pd.to_datetime(df.index).tz_localize(None)

    # 2. Tradegate-Wert holen
    tg_price = get_tradegate_live(isin)
    
    # 3. VERGLEICHS-LOG (Immer sichtbar)
    yahoo_val = df[df['Source'] == 'Yahoo_Legacy']['Price'].iloc[-1]
    print(f"\n=== VERGLEICH ===")
    print(f"Letzter Yahoo-Wert:    {yahoo_val:.2f} €")
    print(f"Aktueller Tradegate:   {tg_price if tg_price else 'FEHLER'} €")
    print(f"==================\n")

    if tg_price:
        # Wir fügen den Wert immer hinzu, wenn er noch nicht exakt so drin steht
        new_row = pd.DataFrame([{'Price': tg_price, 'Source': 'Tradegate_Live'}], 
                              index=[pd.Timestamp.now().tz_localize(None)])
        df = pd.concat([df, new_row])
        
    # Datei IMMER speichern, damit Git sie findet
    df.to_csv(FILENAME)
    print(f"✅ Datei {FILENAME} gespeichert. Zeilen: {len(df)}")

if __name__ == "__main__":
    run()
