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
        """Infiltration der Tradegate-Kursseite"""
        try:
            url = f"https://www.tradegate.de/aktien.php?isin={self.isin}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'}
            res = requests.get(url, headers=headers, timeout=10)
            # Suche nach Ask-Preis im HTML
            match = re.search(r'id="ask"[^>]*>([\d\.,]+)</span>', res.text)
            if match:
                price = float(match.group(1).replace('.', '').replace(',', '.'))
                return price
        except Exception as e:
            self.log(f"Tradegate-Fehler: {e}")
        return None

    def run(self):
        self.log(f"🚀 Sentinel-Check: {self.isin} / {self.symbol}")
        
        # 1. Datenbasis laden (Yahoo Historie)
        if os.path.exists(self.storage_file):
            df = pd.read_csv(self.storage_file, index_col=0)
            df.index = pd.to_datetime(df.index)
        else:
            self.log("Erzeuge neuen Speicher aus Yahoo-Legacy...")
            ticker = yf.Ticker(self.symbol)
            df = ticker.history(period="max")[['Close']]
            df.columns = ['Price']
            df['Source'] = 'Yahoo_Legacy'
            df.index = pd.to_datetime(df.index).tz_localize(None)

        # 2. Live-Abgleich mit Tradegate
        yahoo_last = float(df[df['Source'] == 'Yahoo_Legacy']['Price'].iloc[-1])
        tg_live = self.get_tradegate_live()
        
        if tg_live:
            # Berechnung der Abweichung zum LETZTEN gespeicherten Wert (egal welcher Source)
            current_last = float(df['Price'].iloc[-1])
            diff = abs((tg_live - current_last) / current_last)
            
            # 0,1% Regel für neue Ankerpunkte
            if diff >= 0.001:
                new_row = pd.DataFrame([{'Price': tg_live, 'Source': 'Tradegate_Live'}], 
                                      index=[pd.Timestamp.now().tz_localize(None)])
                df = pd.concat([df, new_row])
                df.to_csv(self.storage_file)
                self.log(f"✅ Neuer Tradegate-Anker gesetzt: {tg_live} €")
            else:
                self.log(f"⏳ Kurs stabil ({tg_live} €). Kein neuer Anker nötig.")

            # 3. DIE VERGLEICHS-BOX (Speziell für deine Auswertung)
            print("\n" + "="*60)
            print(f"📊 SINNHAFTIGKEITS-CHECK (Aureum Sentinel)")
            print(f"Letzter Yahoo-Schlusswert:  {yahoo_last:,.2f} €")
            print(f"Aktueller Tradegate-Kurs:  {tg_live:,.2f} €")
            print(f"Absolute Differenz:        {abs(tg_live - yahoo_last):,.4f} €")
            print(f"Relative Abweichung:       {((tg_live/yahoo_last)-1)*100:.4f}%")
            print("="*60)
            print(f"Letzte 3 Einträge im Speicher:\n{df.tail(3).to_string()}")
            print("="*60 + "\n")

            # 4. Automatischer Git-Push
            try:
                subprocess.run(["git", "config", "user.name", "Aureum-Bot"], check=True)
                subprocess.run(["git", "config", "user.email", "bot@aureum.de"], check=True)
                subprocess.run(["git", "add", self.storage_file], check=True)
                # Nur pushen, wenn es Änderungen gibt
                status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
                if status.stdout:
                    subprocess.run(["git", "commit", "-m", f"Update {self.isin} - {tg_live}€"], check=True)
                    subprocess.run(["git", "push"], check=True)
                    self.log("🚀 Daten-Update erfolgreich gepusht.")
                else:
                    self.log("Keine Änderungen im Dateisystem. Push übersprungen.")
            except Exception as e:
                self.log(f"Git-Fehler: {e}")

if __name__ == "__main__":
    # Siemens Energy Testlauf
    sentinel = AureumSentinelFinal("DE000ENER6Y0", "ENR.DE")
    sentinel.run()
