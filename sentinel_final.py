import os
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime

FILENAME = "sentinel_master_storage.csv"

def get_tradegate_direct(isin):
    """Greift den Kurs direkt ohne HTML-Parsing ab"""
    try:
        # Die direkte Kurs-Schnittstelle von Tradegate
        url = f"https://www.tradegate.de/export.php?isin={isin}&type=csv"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        
        # Tradegate liefert hier meist CSV-Stil: ISIN;Name;Last;Bid;Ask...
        # Wir extrahieren den Last-Preis (3. Spalte)
        data = res.text.strip().split(';')
        if len(data) > 2:
            price = float(data[2].replace(',', '.'))
            return price
    except Exception as e:
        print(f"Direkt-Zugriff fehlgeschlagen: {e}")
    return None

def run():
    isin, symbol = "DE000ENER6Y0", "ENR.DE"
    print(f"🛡️ Direkter Sentinel-Check für {isin}")
    
    # 1. Historie laden
    if os.path.exists(FILENAME):
        df = pd.read_csv(FILENAME, index_col=0)
        df.index = pd.to_datetime(df.index)
    else:
        df = yf.Ticker(symbol).history(period="max")[['Close']]
        df.columns = ['Price']
        df['Source'] = 'Yahoo_Legacy'
        df.index = pd.to_datetime(df.index).tz_localize(None)

    # 2. Direkter Abruf
    tg_price = get_tradegate_direct(isin)
    
    # 3. Vergleich & Validierung
    yahoo_val = df[df['Source'] == 'Yahoo_Legacy']['Price'].iloc[-1]
    
    print(f"\n=== DIRECT DATA INTERFACE ===")
    print(f"Yahoo (Legacy):   {yahoo_val:.2f} €")
    print(f"Tradegate (Direct): {tg_price if tg_price else 'N/A'} €")
    print(f"=============================\n")

    if tg_price:
        # 0,1% Regel (Eiserner Standard)
        last_val = float(df['Price'].iloc[-1])
        if abs((tg_price - last_val) / last_val) >= 0.001:
            new_entry = pd.DataFrame([{'Price': tg_price, 'Source': 'Tradegate_Direct'}], 
                                    index=[pd.Timestamp.now().tz_localize(None)])
            df = pd.concat([df, new_entry])
            df.to_csv(FILENAME)
            print(f"✅ Neuer Direkt-Anker gesetzt.")
        else:
            print("⏳ Abweichung < 0,1%. Kein neuer Anker.")
            # Wir speichern trotzdem, um die CSV für Git aktuell zu halten
            df.to_csv(FILENAME)

if __name__ == "__main__":
    run()
