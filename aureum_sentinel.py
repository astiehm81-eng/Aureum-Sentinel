import pandas as pd
import yfinance as yf
import os
import json
import concurrent.futures
import threading
import time
from datetime import datetime

# --- CONFIG V279 ---
MAX_WORKERS = 150 # Gesunder Mittelwert
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BLACKLIST_FILE = "blacklist.json"

class AureumSentinelV279:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats = {"done": 0, "cleaned": 0, "start": time.time()}
        self.load_resources()

    def load_resources(self):
        # Blacklist laden
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r") as f: self.blacklist = set(json.load(f))
        else: self.blacklist = set()
        
        # Pool laden & Dubletten-Vorfilterung (Heimatmarkt-Logik)
        if not os.path.exists(POOL_FILE): self.pool = []; return
        with open(POOL_FILE, "r") as f: raw_data = json.load(f)
        
        # Nur Ticker behalten, die nicht auf Blacklist sind und Primär-Listings bevorzugen
        refined = {}
        for entry in raw_data:
            t = entry['symbol']
            if t in self.blacklist: continue
            
            base = t.split('.')[0]
            # Logik: Wenn wir 'AAPL' haben, brauchen wir 'AAPL.DE' nicht.
            if base not in refined:
                refined[base] = entry
            else:
                # Bevorzuge US (kein Punkt) oder DE (.DE) vor dem Rest
                current = refined[base]['symbol']
                if '.' not in t: refined[base] = entry
                elif '.DE' in t and ('.' in current and '.DE' not in current):
                    refined[base] = entry
        
        self.pool = list(refined.values())
        self.stats["initial_count"] = len(raw_data)

    def update_blacklist(self, ticker):
        with self.stats_lock:
            self.blacklist.add(ticker)
            with open(BLACKLIST_FILE, "w") as f:
                json.dump(list(self.blacklist), f, indent=4)

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            y_obj = yf.Ticker(ticker)
            # 5 Tage / 5 Minuten Intervall
            df = y_obj.history(period="5d", interval="5m")
            
            if df.empty:
                # Wenn keine Daten kommen: Sofort auf Blacklist (da wir Primär-Listings bevorzugen)
                self.update_blacklist(ticker)
                with self.stats_lock: self.stats["cleaned"] += 1
                return

            # Speicher-Logik (Partitioned Parquet)
            char = ticker[0].upper() if ticker[0].isalpha() else "_"
            path = f"{HERITAGE_ROOT}2020s/2026/{char}_registry.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            df['Ticker'] = ticker
            # Speichern ohne Index (Datum wird Spalte)
            df.reset_index().to_parquet(path, engine='pyarrow', append=os.path.exists(path))
            
            with self.stats_lock: self.stats["done"] += 1
        except:
            pass

    def run(self):
        total = len(self.pool)
        print(f"🚀 V279 Focus-Run | Pool: {total} (Vorher: {self.stats['initial_count']})")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(self.worker_task, self.pool)
            
        elapsed = (time.time() - self.stats['start']) / 60
        print(f"\n✅ FINISHED: {self.stats['done']} erfolgreich | {self.stats['cleaned']} Ticker entfernt | Dauer: {elapsed:.1f}m")

if __name__ == "__main__":
    AureumSentinelV279().run()
