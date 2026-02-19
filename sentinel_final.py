import os
import pandas as pd
import yfinance as yf
import requests
import re
from datetime import datetime

class AureumSentinelTest:
    def __init__(self):
        self.isin = "DE000ENER6Y0"  # Siemens Energy
        self.symbol = "ENR.DE"      # Yahoo-Ticker für Siemens Energy
        self.storage_file = "sentinel_test_enr.parquet"
        
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛡️ {msg}", flush=True)

    def get_tradegate_price(self):
        """Direkter Hard-Refresh vom Tradegate Orderbuch"""
        try:
            url = f"https://www.tradegate.de/aktien.php?isin={self.isin}"
            headers = {'User-Agent': 'Mozilla/5.0'}
            res = requests.get(url, headers=headers, timeout=10)
            # Extraktion des Brief-Kurses (Ask)
            match = re.search(r'id="ask">([\d\.,]+)</span>', res.text)
            if match:
                price = float(match.group(1).replace('.', '').replace(',', '.'))
                return price
        except Exception as e:
            self.log(f"Fehler bei Tradegate-Abfrage: {e}")
        return None

    def run_test(self):
        self.log(f"🚀 Start Testlauf für Siemens Energy ({self.isin})")
        
        # 1. Datenbasis laden oder neu anlegen
        if not os.path.exists(self.storage_file):
            self.log("Kein Speicher gefunden. Initialisiere 30J-Historie via Yahoo...")
            try:
                ticker = yf.Ticker(self.symbol)
                df = ticker.history(period="max")[['Close']] # 'max' für volle Historie
                df.columns = ['Price']
                df['Source'] = 'Yahoo_Legacy'
                df.index = pd.to_datetime(df.index)
                self.log(f"✅ {len(df)} historische Datenpunkte geladen.")
            except Exception as e:
                self.log(f"❌ Yahoo-Fehler: {e}")
                return
        else:
            df = pd.read_parquet(self.storage_file)
            self.log(f"Speicher geladen ({len(df)} Einträge).")

        # 2. Aktuellen Kurs von Tradegate holen
        live_price = self.get_tradegate_price()
        
        if live_price:
            last_price = df['Price'].iloc[-1]
            diff = abs((live_price - last_price) / last_price)
            
            self.log(f"Aktueller Kurs (Tradegate): {live_price} €")
            self.log(f"Abweichung zum letzten Anker: {diff*100:.4f}%")

            # 3. 0,1% Regel anwenden
            if diff >= 0.001:
                new_entry = pd.DataFrame([{
                    'Price': live_price,
                    'Source': 'Tradegate_Live'
                }], index=[pd.Timestamp.now()])
                df = pd.concat([df, new_entry])
                df.to_parquet(self.storage_file, compression='snappy')
                self.log(f"✅ Neuer Ankerpunkt bei {live_price} € gespeichert.")
            else:
                self.log("⏳ Preisbewegung < 0,1%. Kein neuer Ankerpunkt erforderlich.")
        else:
            self.log("❌ Konnte keinen aktuellen Kurs von Tradegate abrufen.")

if __name__ == "__main__":
    AureumSentinelTest().run_test()
