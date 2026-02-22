import pandas as pd
import yfinance as yf
import os
import json
import concurrent.futures
import threading
import time
from datetime import datetime

# --- CONFIG V281 (STEADY-FLOW) ---
MAX_WORKERS = 100 
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BLACKLIST_FILE = "blacklist.json"
AUDIT_FILE = "heritage_audit.txt"

class AureumSentinelV281:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats = {
            "done": 0, 
            "err": 0, 
            "start": time.time(), 
            "total_pool": 0,
            "blacklisted_now": 0
        }
        self.load_resources()

    def clean_ticker(self, ticker):
        if not ticker: return None
        t = ticker.replace('$', '').strip()
        parts = t.split('.')
        return f"{parts[0]}.{parts[1]}" if len(parts) > 2 else t

    def load_resources(self):
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r") as f: self.blacklist = set(json.load(f))
        else: self.blacklist = set()
        
        if not os.path.exists(POOL_FILE): self.pool = []; return
        with open(POOL_FILE, "r") as f: raw_data = json.load(f)
        
        refined = {}
        for entry in raw_data:
            t = self.clean_ticker(entry.get('symbol', ''))
            if not t or t in self.blacklist: continue
            base = t.split('.')[0]
            if base not in refined or '.' not in t: refined[base] = t
            elif '.DE' in t and '.' in refined[base]: refined[base] = t
        
        self.pool = [{"symbol": s} for s in refined.values()]
        self.stats["total_pool"] = len(self.pool)

    def print_dashboard(self):
        with self.stats_lock:
            elapsed = (time.time() - self.stats['start']) / 60
            done = self.stats['done']
            total = self.stats['total_pool']
            speed = done / elapsed if elapsed > 0 else 0
            coverage = (done / total * 100) if total > 0 else 0
            
            print(f"\n[DASHBOARD] {datetime.now().strftime('%H:%M:%S')}")
            print(f"Progress: [{done}/{total}] ({coverage:.1f}%) | Speed: {speed:.1f} a/m")
            print(f"Status:   {self.stats['err']} Errors | {self.stats['blacklisted_now']} Blacklisted\n", flush=True)

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # Batch-Download für Stabilität
            df = yf.download(ticker, period="5d", interval="5m", progress=False, group_by='ticker', timeout=10)
            
            if df.empty:
                with self.stats_lock:
                    self.blacklist.add(ticker)
                    self.stats["blacklisted_now"] += 1
                return

            char = ticker[0].upper() if ticker[0].isalpha() else "_"
            path = f"{HERITAGE_ROOT}2020s/2026/{char}_registry.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            df_save = df.reset_index()
            df_save['Ticker'] = ticker
            
            # Atomic Write
            with self.stats_lock:
                if os.path.exists(path):
                    # Nur neue Daten anfügen (Delta)
                    existing = pd.read_parquet(path)
                    pd.concat([existing, df_save]).drop_duplicates(subset=['Date', 'Ticker']).to_parquet(path, index=False)
                else:
                    df_save.to_parquet(path, index=False)
                self.stats["done"] += 1
        except:
            with self.stats_lock: self.stats["err"] += 1

    def run(self):
        print(f"🚀 V281 Start | Pool: {self.stats['total_pool']} | Workers: {MAX_WORKERS}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Batches von 50 für regelmäßige Dashboard-Updates
            for i in range(0, len(self.pool), 50):
                batch = self.pool[i:i+50]
                executor.map(self.worker_task, batch)
                self.print_dashboard()
                
        # Blacklist final speichern
        with open(BLACKLIST_FILE, "w") as f:
            json.dump(list(self.blacklist), f, indent=4)

if __name__ == "__main__":
    AureumSentinelV281().run()
