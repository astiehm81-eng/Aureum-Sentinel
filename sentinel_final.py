import os
import pandas as pd
import yfinance as yf
import requests
import re
import time
from datetime import datetime

class AureumSentinelCore:
    def __init__(self):
        # Konfiguration für Siemens Energy Testlauf
        self.isin = "DE000ENER6Y0"
        self.symbol = "ENR.DE"
        self.storage_file = "sentinel_storage_enr.parquet"
        
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛡️ {msg}", flush=True)

    def get_tradegate_price(self):
        """Hard-Refresh direkt aus dem HTML-Quelltext von Tradegate"""
        try:
            url = f"https://www.tradegate.de/aktien.php?isin={self.isin}"
            # Tarnung als echter Browser, um Blockaden zu vermeiden
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
            }
            
            res = requests.get(url, headers=headers, timeout=15)
            
            # Suche nach dem Brief-Kurs (Ask)
            match = re.search(r'id="ask">([\d\.,]+)</span>', res.text)
            
            if match:
                # Konvertiert deutsches Format (1.234,56) in Float (1234.56)
                price_str = match.group(1).replace('.', '').replace(',', '.')
                return float(price_str)
            else:
                self.log("⚠️ Feld 'id=ask' nicht gefunden. Prüfe alternatives Muster...")
                # Backup-Suche im Tabellen-Layout
                match_alt = re.search(r'Brief.*?([\d\.,]+)</td>', res.text, re.DOTALL)
                if match_alt:
                    return float(match_alt.group(1).replace('.', '').replace(',', '.'))
                    
        except Exception as e:
            self.log(f"❌ Fehler bei Tradegate-Abfrage: {e}")
        return None

    def run(self):
        self.log(f"🚀 Aureum Sentinel Lauf startet: {self.isin}")
        
        # 1. Datenbasis laden oder 30J-Historie initialisieren
        if not os.path.exists(self.storage_file):
            self.log("Kein Speicher gefunden. Initialisiere 30J-Historie via Yahoo...")
            try:
                ticker = yf.Ticker(self.symbol)
                # Holt die maximale verfügbare Historie
                df = ticker.history(period="max")[['Close']]
                df.columns = ['Price']
                df['Source'] = 'Yahoo_Legacy'
                df.index = pd.to_datetime(df.index)
                self.log(f"✅ {len(df)} historische Datenpunkte von Yahoo geladen.")
            except Exception as e:
                self.log(f"❌ Kritischer Fehler beim Yahoo-Import: {e}")
                return
        else:
            # Effizientes Einlesen des Parquet-Speichers
            df = pd.read_parquet(self.storage_file)
            self.log(f"Speicher geladen ({len(df)} Einträge).")

        # 2. Aktuellen Kurs von Tradegate abrufen
        live_price = self.get_tradegate_price()
        
        if live_price:
            last_recorded_price = df['Price'].iloc[-1]
            # Berechnung der Abweichung für die 0,1%-Regel
            diff = abs((live_price - last_recorded_price) / last_recorded_price)
            
            self.log(f"Aktueller Tradegate-Kurs: {live_price} €")
            self.log(f"Abweichung zum letzten Anker: {diff*100:.4f}%")

            # 3. Ankerpunkt-Logik (Eiserner Standard)
            if diff >= 0.001:
                new_entry = pd.DataFrame([{
                    'Price': live_price,
                    'Source': 'Tradegate_Live'
                }], index=[pd.Timestamp.now()])
                
                df_updated = pd.concat([df, new_entry])
                # Speichern im platzsparenden Parquet-Format
                df_updated.to_parquet(self.storage_file, compression='snappy')
                self.log(f"✅ Neuer Ankerpunkt bei {live_price} € im Speicher fixiert.")
            else:
                self.log("⏳ Preisbewegung innerhalb des 0,1% Rauschfilters. Kein Update nötig.")
        else:
            self.log("❌ Abbruch: Konnte keine validen Live-Daten von Tradegate extrahieren.")

if __name__ == "__main__":
    sentinel = AureumSentinelCore()
    sentinel.run()
