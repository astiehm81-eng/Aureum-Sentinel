import pandas as pd
import yfinance as yf
import os
import json
import time
from datetime import datetime

# --- KONFIGURATION (ZURÜCK ZUM STABILEN KERN) ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BLACKLIST_FILE = "blacklist.json"
BATCH_SIZE = 50  # Yahoo mag Gruppen von 50 Tickern gleichzeitig sehr gern

class AureumSentinelV283:
    def __init__(self):
        self.stats = {"done": 0, "blacklisted": 0, "start": time.time()}
        self.load_resources()

    def clean_ticker(self, ticker):
        if not ticker: return None
        t = ticker.replace('$', '').strip()
        # Entferne Duplikate in der Endung (Fix für .PA.PA)
        parts = t.split('.')
        return f"{parts[0]}.{parts[1]}" if len(parts) > 2 else t

    def load_resources(self):
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r") as f: self.blacklist = set(json.load(f))
        else: self.blacklist = set()
        
        if not os.path.exists(POOL_FILE): 
            self.pool_tickers = []
            return
            
        with open(POOL_FILE, "r") as f: raw_data = json.load(f)
        
        # Validierung und Priorisierung (US > DE > Rest)
        refined = {}
        for entry in raw_data:
            t = self.clean_ticker(entry.get('symbol', ''))
            if not t or t in self.blacklist: continue
            base = t.split('.')[0]
            if base not in refined or '.' not in t: refined[base] = t
            elif '.DE' in t and '.' in refined[base]: refined[base] = t
        
        self.pool_tickers = list(refined.values())
        print(f"✅ Pool bereit: {len(self.pool_tickers)} Assets (Säuberung aktiv)")

    def save_data(self, ticker, data):
        """Speichert die Daten im Heritage-Format"""
        if data.empty: return False
        try:
            char = ticker[0].upper() if ticker[0].isalpha() else "_"
            path = f"{HERITAGE_ROOT}2020s/2026/{char}_registry.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            df_save = data.reset_index()
            df_save['Ticker'] = ticker
            
            if os.path.exists(path):
                existing = pd.read_parquet(path)
                pd.concat([existing, df_save]).drop_duplicates(subset=['Date', 'Ticker']).to_parquet(path, index=False)
            else:
                df_save.to_parquet(path, index=False)
            return True
        except:
            return False

    def run(self):
        print(f"🚀 Aureum Sentinel V283 startet Batch-Processing...")
        
        # Wir gehen in 50er Schritten vor, um Yahoo nicht zu reizen
        for i in range(0, len(self.pool_tickers), BATCH_SIZE):
            batch = self.pool_tickers[i:i+BATCH_SIZE]
            ticker_string = " ".join(batch)
            
            try:
                # Der goldene Weg: Ein einziger Call für 50 Ticker
                data = yf.download(ticker_string, period="5d", interval="5m", group_by='ticker', progress=False, threads=False)
                
                for ticker in batch:
                    # Extrahiere Ticker-Daten aus dem Batch-Frame
                    ticker_data = data[ticker] if len(batch) > 1 else data
                    
                    if ticker_data.empty or ticker_data.dropna().empty:
                        self.blacklist.add(ticker)
                        self.stats["blacklisted"] += 1
                    else:
                        if self.save_data(ticker, ticker_data):
                            self.stats["done"] += 1
                            
                # Kleiner Cool-down zwischen den Batches
                time.sleep(2)
                
                # Fortschritt loggen
                elapsed = (time.time() - self.stats['start']) / 60
                print(f"📊 [{i+len(batch)}/{len(self.pool_tickers)}] | Erfolgreich: {self.stats['done']} | Blacklisted: {self.stats['blacklisted']} | {elapsed:.1f}m")
                
            except Exception as e:
                print(f"⚠️ Batch-Fehler bei {batch[0]}...: {e}")
                time.sleep(5)

        # Blacklist finalisieren
        with open(BLACKLIST_FILE, "w") as f:
            json.dump(list(self.blacklist), f, indent=4)
        print("🏁 Zyklus beendet.")

if __name__ == "__main__":
    AureumSentinelV283().run()
