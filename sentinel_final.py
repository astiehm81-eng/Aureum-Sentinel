import os
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime

FILENAME = "sentinel_master_storage.csv"

def get_tradegate_ultra_direct(isin):
    try:
        url = f"https://www.tradegate.de/refresh.php?isin={isin}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        data = res.text.split('|')
        if len(data) > 2:
            return float(data[2].replace('.', '').replace(',', '.'))
    except: return None
    return None

def run_sentinel():
    assets = {
        "DE0007164600": {"symbol": "SAP.DE", "name": "SAP SE"},
        "DE000ENER6Y0": {"symbol": "ENR.DE", "name": "Siemens Energy"}
    }
    
    # Speicher laden
    if os.path.exists(FILENAME):
        master_df = pd.read_csv(FILENAME, index_col=0)
        master_df.index = pd.to_datetime(master_df.index)
    else:
        master_df = pd.DataFrame(columns=['Price', 'Source', 'ISIN'])

    print(f"\n🛡️ SINNHAFTIGKEITS-ABGLEICH")
    print(f"{'Asset':<18} | {'Yahoo (Last)':<12} | {'Tradegate':<12} | {'Status'}")
    print("-" * 65)

    for isin, info in assets.items():
        # Yahoo Check (Robust)
        ticker = yf.Ticker(info['symbol'])
        # Wir nehmen '1mo' um sicher einen Wert zu haben, ziehen aber nur den letzten
        hist = ticker.history(period="1mo")
        y_price = hist['Close'].iloc[-1] if not hist.empty else None
        
        # Tradegate Check
        tg_price = get_tradegate_ultra_direct(isin)
        
        # Anzeige
        y_str = f"{y_price:.2f}" if y_price else "N/A"
        t_str = f"{tg_price:.2f}" if tg_price else "N/A"
        
        # Logik: Nur speichern wenn Tradegate liefert
        if tg_price:
            # Check 0,1% Regel gegen den LETZTEN Eintrag DIESER ISIN
            last_entry = master_df[master_df['ISIN'] == isin]
            last_price = last_entry['Price'].iloc[-1] if not last_entry.empty else 0
            
            diff = abs((tg_price - last_price) / last_price) if last_price > 0 else 1
            
            if diff >= 0.001:
                new_row = pd.DataFrame([{
                    'Price': tg_price, 'Source': 'Tradegate_Direct', 'ISIN': isin
                }], index=[pd.Timestamp.now().tz_localize(None)])
                master_df = pd.concat([master_df, new_row])
                status = "✅ NEUER ANKER"
            else:
                status = "⏳ STABIL"
        else:
            status = "❌ TG-BLOCK"

        print(f"{info['name']:<18} | {y_str:>12} | {t_str:>12} | {status}")

    master_df.to_csv(FILENAME)
    print("-" * 65)
    print(f"Speichergröße: {len(master_df)} Einträge")

if __name__ == "__main__":
    run_sentinel()
