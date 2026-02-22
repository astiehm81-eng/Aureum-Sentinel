import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
from datetime import datetime
from collections import defaultdict

# --- CONFIG V288 (VELOCITY) ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BUFFER_SIZE = 25 # Schreibt alle 25 Assets die Daten weg

class AureumSentinelV288:
    def __init__(self):
        self.stats = {"done": 0, "start": time.time()}
        self.buffer = defaultdict(list) # Sammelt Daten pro Buchstabe
        self.load_pool()

    def log(self, message):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] {message}", flush=True)
        sys.stdout.flush()

    def load_pool(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f:
            data = json.load(f)
            self.pool = [e['symbol'] for e in data if 'symbol' in e]
        self.log(f"Velocity Pool: {len(self.pool)} Assets.")

    def flush_buffer(self):
        """Schreibt alle gesammelten Daten im Buffer effizient auf die Platte"""
        for char, data_list in self.buffer.items():
            path = f"{HERITAGE_ROOT}2020s/2026/{char}_registry.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            new_data = pd.concat(data_list)
            
            if os.path.exists(path):
                existing = pd.read_parquet(path)
                # Vektorisierte Deduplizierung im RAM ist extrem schnell
                combined = pd.concat([existing, new_data]).drop_duplicates(subset=['Date', 'Ticker'])
                combined.to_parquet(path, engine='pyarrow', index=False, compression='snappy')
            else:
                new_data.to_parquet(path, engine='pyarrow', index=False, compression='snappy')
        
        self.buffer.clear()
        self.log("BUFFER FLUSHED: Festplatte synchronisiert.")

    def run(self):
        self.log("START VELOCITY SYNC...")
        
        for idx, ticker in enumerate(self.pool):
            try:
                # Goldener Standard Download (Unverändert stabil)
                y_obj = yf.Ticker(ticker)
                recent_df = y_obj.history(period="2d", interval="5m").reset_index()
                
                if recent_df.empty:
                    continue

                recent_df['Ticker'] = ticker
                if 'Datetime' in recent_df.columns:
                    recent_df = recent_df.rename(columns={'Datetime': 'Date'})

                # In den Buffer statt auf die Platte
                char = ticker[0].upper() if ticker[0].isalpha() else "_"
                self.buffer[char].append(recent_df)
                self.stats["done"] += 1

                # Periodischer Flush zur Sicherheit und Speed-Optimierung
                if len(self.buffer) >= BUFFER_SIZE or (idx + 1) == len(self.pool):
                    self.flush_buffer()
                    self.log(f"Progress: {idx+1}/{len(self.pool)} | OK")

                # Minimaler Sleep für Yahoo-Stabilität
                time.sleep(0.3) 

            except Exception as e:
                self.log(f"SKIP {ticker}: {str(e)[:40]}")

        elapsed = (time.time() - self.stats['start']) / 60
        self.log(f"FINISH: {self.stats['done']} Assets in {elapsed:.1f} Min.")

if __name__ == "__main__":
    AureumSentinelV288().run()
