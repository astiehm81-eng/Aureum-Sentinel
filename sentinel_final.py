import os
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime

FILENAME = "sentinel_master_storage.csv"

def get_tradegate_ultra_direct(isin):
    """Greift den Kurs über die schlanke Ticker-Schnittstelle ab"""
    try:
        url = f"https://www.tradegate.de/refresh.php?isin={isin}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        data = res.text.split('|')
        if len(data) > 2:
            # Last-Preis extrahieren und formatieren
            price_str = data[2].replace('.', '').replace(',', '.')
            return float(price_str)
    except: return None
    return None

def run_sentinel():
    # Definition der Ziel-Assets
    assets = {
        "DE000ENER6Y0": {"symbol": "ENR.DE", "name": "Siemens Energy"},
        "DE0007164600": {"symbol": "SAP.DE", "name": "SAP SE"}
    }
    
    print(f"🛡️ Sentinel Multi-Check gestartet: {datetime.now().strftime('%H:%M:%S')}")
    
    # Vorhandenen Speicher laden oder leeren DataFrame erstellen
    if os.path.exists(FILENAME):
        master_df = pd.read_csv(FILENAME, index_col=0)
        master_df.index = pd.to_datetime(master_df.index)
    else:
        master_df = pd.DataFrame(columns=['Price', 'Source', 'ISIN'])

    print(f"\n{'ASSET':<20} | {'YAHOO (LEGACY)':<15} | {'TRADEGATE LIVE':<15} | {'DIFF'}")
    print("-" * 70)

    for isin, info in assets.items():
        # 1. Yahoo-Referenzwert (letzter bekannter Schlusskurs)
        try:
            ticker = yf.Ticker(info['symbol'])
            yahoo_last = ticker.history(period="1d")['Close'].iloc[-1]
        except: yahoo_last = 0.0

        # 2. Tradegate Ultra-Direkt
        tg_price = get_tradegate_ultra_direct(isin)
        
        # 3. Anzeige & Analyse
        if tg_price and yahoo_last > 0:
            diff = ((tg_price / yahoo_last) - 1) * 100
            print(f"{info['name']:<20} | {yahoo_last:>12.2f} € | {tg_price:>12.2f} € | {diff:>+6.2f}%")
            
            # 4. In Master-Speicher schreiben (Eiserner Standard 0,1% Regel)
            # Wir suchen den letzten Preis für genau diese ISIN im Speicher
            isin_data = master_df[master_df['ISIN'] == isin]
            last_stored_price = isin_data['Price'].iloc[-1] if not isin_data.empty else 0.0
            
            if abs((tg_price - last_stored_price) / (last_stored_price if last_stored_price > 0 else 1)) >= 0.001:
                new_entry = pd.DataFrame([{
                    'Price': tg_price, 
                    'Source': 'Tradegate_Direct', 
                    'ISIN': isin
                }], index=[pd.Timestamp.now().tz_localize(None)])
                master_df = pd.concat([master_df, new_entry])
        else:
            print(f"{info['name']:<20} | {'FEHLER':>12} | {'FEHLER':>12} | ---")

    # Finales Speichern
    master_df.to_csv(FILENAME)
    print("-" * 70)
    print(f"✅ Speicher aktualisiert. Datensätze gesamt: {len(master_df)}")

if __name__ == "__main__":
    run_sentinel()
