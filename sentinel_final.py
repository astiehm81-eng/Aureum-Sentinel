import os
import pandas as pd
import yfinance as yf
import requests
import re
from datetime import datetime

class AureumSentinelTradegateCore:
    def __init__(self, isin, symbol):
        self.isin = isin
        self.symbol = symbol
        self.storage_file = f"sentinel_storage_{isin}.parquet"
        
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛡️ {msg}", flush=True)

    def get_tradegate_live_price(self):
        """Die Methode von gestern: Direkte Infiltration der Kursseite"""
        try:
            url = f"https://www.tradegate.de/aktien.php?isin={self.isin}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            }
            res = requests.get(url, headers=headers, timeout=10)
            
            # Wir suchen exakt den Ask-Wert im HTML-Quelltext
            match = re.search(r'id="ask"[^>]*>([\d\.,]+)</span>', res.text)
            if match:
                return float(match.group(1).replace('.', '').replace(',', '.'))
        except Exception as e:
            self.log(f"Fehler beim Live-Kurs: {e}")
        return None

    def run(self):
        self.log(f"🚀 Starte System (Tradegate-Fokus) für {self.isin}")
        
        # 1. Historischer Speicher (Yahoo Legacy)
        if not os.path.exists(self.storage_file):
            self.log("Erzeuge neuen Speicher mit 30J Yahoo-Historie...")
            df = yf.Ticker(self.symbol).history(period="max")[['Close']]
            df.columns = ['Price']
            df['Source'] = 'Yahoo_Legacy'
            df.index = pd.to_datetime(df.index).tz_localize(None)
        else:
            df = pd.read_parquet(self.storage_file)

        # 2. Den Tradegate-Wert von Gestern/Heute abgreifen
        current_tg = self.get_tradegate_live_price()
        
        if current_tg:
            last_price = df['Price'].iloc[-1]
            diff = abs((current_tg - last_price) / last_price)
            
            # 0,1% Ankerpunkt-Regel
            if diff >= 0.001:
                new_row = pd.DataFrame([{'Price': current_tg, 'Source': 'Tradegate_Live'}], 
                                      index=[pd.Timestamp.now().tz_localize(None)])
                df = pd.concat([df, new_row])
                self.log(f"✅ Ankerpunkt bei {current_tg} € gesetzt.")
            else:
                self.log(f"⏳ Kurs stabil ({current_tg} €).")
        
        # 3. Versiegeln
        df.to_parquet(self.storage_file, compression='snappy')
        self.log("🛡️ Daten im Speicher gesichert.")

if __name__ == "__main__":
    sentinel = AureumSentinelTradegateCore("DE000ENER6Y0", "ENR.DE")
    sentinel.run()
