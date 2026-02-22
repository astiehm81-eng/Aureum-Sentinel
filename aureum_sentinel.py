import pandas as pd
import yfinance as yf
import os
import json
import concurrent.futures
import threading
import time
from datetime import datetime

# --- CONFIG V278 ---
MAX_WORKERS = 100 # Reduziert von 300 auf 100, um Insta-Bans zu vermeiden
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BLACKLIST_FILE = "blacklist.json"
AUDIT_FILE = "heritage_audit.txt"

class AureumSentinelV278:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats = {"done": 0, "err": 0, "start": time.time()}
        self.init_files()

    def init_files(self):
        # WICHTIG: Blacklist leeren für den Neustart
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "w") as f:
                json.dump([], f)
        
    def worker_task(self, asset, total):
        ticker = asset['symbol']
        try:
            # Jitter: Verhindert, dass 100 Anfragen exakt gleichzeitig einschlagen
            time.sleep(os.getpid() % 10 * 0.1) 
            
            y_obj = yf.Ticker(ticker)
            df = y_obj.history(period="5d", interval="5m")
            
            if df.empty:
                raise ValueError("No Data")

            # Partitioniertes Speichern (V277 Standard)
            char = ticker[0].upper() if ticker[0].isalpha() else "_"
            path = f"{HERITAGE_ROOT}2020s/2026/{char}_registry.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            with self.stats_lock:
                df['Ticker'] = ticker
                df.to_parquet(path, engine='pyarrow', append=os.path.exists(path))
                self.stats["done"] += 1
                
            if self.stats["done"] % 20 == 0:
                self.log_status(total)
        except:
            with self.stats_lock: self.stats["err"] += 1

    def log_status(self, total):
        elapsed = (time.time() - self.stats['start']) / 60
        speed = self.stats['done'] / elapsed if elapsed > 0 else 0
        coverage = (self.stats['done'] / total) * 100
        print(f"📊 Progress: {self.stats['done']}/{total} ({coverage:.1f}%) | Speed: {speed:.1f} a/m | Errs: {self.stats['err']}")

    def run(self):
        with open(POOL_FILE, "r") as f:
            pool = json.load(f)
        
        total = len(pool)
        print(f"🚀 Sentinel V278 Start | Pool: {total} | Workers: {MAX_WORKERS}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(lambda a: self.worker_task(a, total), pool)

if __name__ == "__main__":
    AureumSentinelV278().run()
