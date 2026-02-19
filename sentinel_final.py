import os
import pandas as pd
import yfinance as yf
import requests
import re
import subprocess
from datetime import datetime

class AureumSentinelFullAuto:
    def __init__(self, isin, symbol):
        self.isin = isin
        self.symbol = symbol
        self.storage_file = "sentinel_master_storage.csv"
        
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛡️ {msg}", flush=True)

    def git_push(self):
        """Überträgt die Änderungen automatisch ins GitHub Repository"""
        try:
            subprocess.run(["git", "config", "user.name", "Aureum Sentinel Bot"], check=True)
            subprocess.run(["git", "config", "user.email", "bot@aureum-sentinel.com"], check=True)
            subprocess.run(["git", "add", self.storage_file], check=True)
            # Commit-Nachricht mit Zeitstempel
            commit_msg = f"Sentinel Update: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            subprocess.run(["git", "push"], check=True)
            self.log("🚀 Daten erfolgreich ins Repository gepusht.")
        except Exception as e:
            self.log(f"⚠️ Git-Push fehlgeschlagen: {e}")

    def get_tradegate_live(self):
        """Tradegate Infiltration"""
        try:
            url = f"https://www.tradegate.de/aktien.php?isin={self.isin}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'}
            res = requests.get(url, headers=headers, timeout=10)
            match = re.search(r'id="ask"[^>]*>([\d\.,]+)</span>', res.text)
            if match:
                return float(match.group(1).replace('.', '').replace(',', '.'))
        except: return None
        return None

    def run(self):
        self.log(f"🔄 Start im Full-Auto-Modus: {self.isin}")
        
        # 1. Daten laden/initialisieren
        if os.path.exists(self.storage_file):
            df = pd.read_csv(self.storage_file, index_col=0)
            df.index = pd.to_datetime(df.index)
        else:
            self.log("Initialisiere 30J Yahoo-Basis...")
            df = yf.Ticker(self.symbol).history(period="max")[['Close']]
            df.columns = ['Price']
            df['Source'] = 'Yahoo_Legacy'
            df.index = pd.to_datetime(df.index).tz_localize(None)

        # 2. Live-Check & 0,1% Regel
        live_price = self.get_tradegate_live()
        if live_price:
            last_price = float(df['Price'].iloc[-1])
            diff = abs((live_price - last_price) / last_price)
            
            if diff >= 0.001:
                new_row = pd.DataFrame([{'Price': live_price, 'Source': 'Tradegate_Live'}], 
                                      index=[pd.Timestamp.now().tz_localize(None)])
                df = pd.concat([df, new_row])
                df.to_csv(self.storage_file)
                self.log(f"✅ Ankerpunkt gesetzt ({live_price} €). Starte Push...")
                
                # 3. Automatischer Push
                self.git_push()
            else:
                self.log(f"⏳ Kurs stabil ({live_price} €). Kein Push nötig.")

        # 4. Daten-Interface für meine Auswertung (wie besprochen)
        print("\n=== DATA INTERFACE ===")
        print(df.tail(5).to_string())
        print("======================\n")

if __name__ == "__main__":
    AureumSentinelFullAuto("DE000ENER6Y0", "ENR.DE").run()
