import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta

ASSETS = {
    "DE0007164600": {"ticker": "SAP.DE", "name": "SAP SE"},
    "DE000ENER610": {"ticker": "ENR.DE", "name": "Siemens Energy"},
}

def get_tradegate_history_check(isin):
    # Wir ziehen hier die Intraday- und Schlusskurse der letzten Tage von Tradegate
    url = f"https://www.tradegate.de/refresh.php?isin={isin}"
    try:
        res = requests.get(url, timeout=5)
        parts = res.text.split('|')
        if len(parts) > 2:
            return float(parts[2].replace(',', '.'))
    except: return None

def calibrate_and_sync():
    all_records = []
    for isin, info in ASSETS.items():
        print(f"🔍 Abgleich: {info['name']}...")
        
        # 1. Yahoo-Daten laden (1 Monat für den Abgleich)
        stock = yf.Ticker(info['ticker'])
        df_yahoo = stock.history(period="1mo", interval="1d")
        
        # 2. Aktuellen Tradegate-Wert holen (Deine 171,53 € bzw. 165,xx €)
        tg_price = get_tradegate_history_check(isin)
        
        # 3. DER ABGLEICH: 
        # Wir löschen den Yahoo-Wert für HEUTE komplett, da er (wie bei SAP) falsch sein kann.
        today = datetime.now().strftime('%Y-%m-%d')
        
        for ts, row in df_yahoo.iterrows():
            ts_str = ts.strftime('%Y-%m-%d')
            
            # Falls heute: Nur Tradegate zählt!
            if ts_str == today and tg_price:
                price = tg_price
                source = "TRADEGATE_MASTER"
            else:
                price = round(row['Close'], 2)
                source = "YAHOO_BACKFILL"
            
            all_records.append([ts_str, isin, info['name'], price, source])
            
    # Speichern und Bereinigen
    df_final = pd.DataFrame(all_records, columns=['Timestamp', 'ISIN', 'Asset', 'Price', 'Source'])
    df_final.to_csv('sentinel_history.csv', index=False)
    print("✅ Schnittstellen-Abgleich abgeschlossen. Tradegate hat die Kontrolle.")

calibrate_and_sync()
