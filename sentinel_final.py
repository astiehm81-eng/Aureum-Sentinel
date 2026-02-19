import os
import pandas as pd
import yfinance as yf
import requests
import re
import subprocess
from datetime import datetime

class AureumSentinelFinal:
    def __init__(self, isin, symbol):
        self.isin = isin
        self.symbol = symbol
        self.storage_file = "sentinel_master_storage.csv"
        
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛡️ {msg}", flush=True)

    def get_tradegate_live(self):
        try:
            url = f"https://www.tradegate.de/aktien.php?isin={self.isin}"
            headers = {'User-Agent': 'Mozilla/5.0'}
            res = requests.get(url, headers=headers, timeout=10)
            match = re.search(r'id="ask"[^>]*>([\d\.,]+)</span>', res.text)
            return float(match.group(1).replace('.', '').replace(',', '.')) if match else None
        except: return None

    def run(self):
        self.log(f"🚀 Start Full-Auto für {self.isin}")
        
        if os.path.exists(self.storage_file):
            df = pd.read_csv(self.storage_file, index_col=0)
            df.index = pd.to_datetime(df.index)
        else:
            ticker = yf.Ticker(self.symbol)
            df = ticker.history(period="max")[['Close']]
            df.columns = ['Price']
            df['Source'] = 'Yahoo_Legacy'
            df.index = pd.to_datetime(df.index).tz_localize(None)

        live_price = self.get_tradegate_live()
        if live_price:
            new_row = pd.DataFrame([{'Price': live_price, 'Source': 'Tradegate_Live'}], 
                                  index=[pd.Timestamp.now().tz_localize(None)])
            df = pd.concat([df, new_row])
            df.to_csv(self.storage_file)
            self.log(f"✅ CSV aktualisiert: {live_price} €")

            # AUTO-PUSH LOGIK
            try:
                subprocess.run(["git", "config", "user.name", "GitHub Action"], check=True)
                subprocess.run(["git", "config", "user.email", "action@github.com"], check=True)
                subprocess.run(["git", "add", self.storage_file], check=True)
                subprocess.run(["git", "commit", "-m", "Sentinel Auto-Update"], check=True)
                subprocess.run(["git", "push"], check=True)
                self.log("🚀 Datei erfolgreich ins Repo gepusht!")
            except Exception as e:
                self.log(f"⚠️ Push fehlgeschlagen (evtl. keine Änderungen): {e}")

        # Wichtig für meine Auswertung:
        print("\n=== DATA PREVIEW ===")
        print(df.tail(5).to_string())
        print("====================\n")

if __name__ == "__main__":
    AureumSentinelFinal("DE000ENER6Y0", "ENR.DE").run()
