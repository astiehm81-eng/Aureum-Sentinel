import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
import threading
import time
import random
from datetime import datetime

# --- KONFIGURATION ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
BLACKLIST_FILE = "blacklist.json"
MAX_WORKERS = 150
BATCH_SIZE = 200
ANCHOR_THRESHOLD = 0.0005 

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.memory_buffer = [] 
        self.buffer_lock = threading.Lock()
        # Fehler-Tracking für Blacklist
        self.error_registry = {} 
        self.stats = {"processed": 0, "errors": 0, "start": time.time()}

    def log(self, level, ticker, message):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] [{level:7}] {ticker:10} | {message}"
        print(line, flush=True)
        with self.stats_lock:
            with open(AUDIT_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.init_structure()
        self.load_blacklist()
        self.load_and_clean_pool()

    def init_structure(self):
        for d in ["1960s","1970s","1980s","1990s", "2000s", "2010s", "2020s"]:
            os.makedirs(os.path.join(HERITAGE_ROOT, d), exist_ok=True)

    def load_blacklist(self):
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r") as f: self.blacklist = set(json.load(f))
        else: self.blacklist = set()

    def update_blacklist(self, ticker):
        """Ticker nach 3 Fehlern permanent verbannen"""
        with inspector.stats_lock:
            count = inspector.error_registry.get(ticker, 0) + 1
            inspector.error_registry[ticker] = count
            if count >= 3:
                self.blacklist.add(ticker)
                with open(BLACKLIST_FILE, "w") as f:
                    json.dump(list(self.blacklist), f, indent=4)
                inspector.log("BLACK", ticker, "Permanent verbannt (3 Fehler).")

    def worker_task(self, asset):
        ticker = asset['symbol']
        
        # --- EXPLIZITER JITTER ---
        # Versatz zwischen 50ms und 500ms, um API-Bursts zu glätten
        time.sleep(random.uniform(0.01, 0.05))
        
        try:
            # Stooq Abfrage
            r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=5)
            hist = pd.read_csv(io.StringIO(r.text)) if r.status_code == 200 else pd.DataFrame()
            
            # Yahoo Abfrage
            recent = yf.Ticker(ticker).history(period="7d", interval="5m").reset_index()
            
            df = pd.concat([self.clean_df(hist), self.clean_df(recent)])
            if df.empty: raise ValueError("Keine Daten")

            # Eiserner Standard
            df = df.sort_values('Date').drop_duplicates(subset=['Date'])
            anchors = [df.iloc[0].to_dict()]
            for i in range(1, len(df)):
                if abs((df.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR_THRESHOLD:
                    anchors.append(df.iloc[i].to_dict())
            
            res_df = pd.DataFrame(anchors)
            res_df['Ticker'] = ticker
            
            with inspector.buffer_lock:
                inspector.memory_buffer.append(res_df)
            
            with inspector.stats_lock: inspector.stats["processed"] += 1
            
        except:
            with inspector.stats_lock: inspector.stats["errors"] += 1
            self.update_blacklist(ticker)

    def flush_buffer_to_disk(self):
        if not inspector.memory_buffer: return
        
        with inspector.buffer_lock:
            big_df = pd.concat(inspector.memory_buffer)
            inspector.memory_buffer = []

        for year, group in big_df.groupby(big_df['Date'].dt.year):
            decade = f"{(int(year)//10)*10}s"
            path = os.path.join(HERITAGE_ROOT, decade, f"{int(year)}.parquet")
            
            db = pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame()
            tickers_in_batch = group['Ticker'].unique()
            if not db.empty:
                db = db[~db['Ticker'].isin(tickers_in_batch)]
            
            pd.concat([db, group], ignore_index=True).to_parquet(path, index=False, compression='snappy')

    def clean_df(self, df):
        if df is None or df.empty: return pd.DataFrame()
        if 'Date' not in df.columns and 'Datetime' in df.columns:
            df = df.rename(columns={'Datetime': 'Date'})
        if 'Date' not in df.columns: return pd.DataFrame()
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        return df

    def load_and_clean_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: raw_pool = json.load(f)
            # Blacklist Filter
            self.pool = [a for a in raw_pool if a['symbol'] not in self.blacklist]
            
            # Smart Deduplication
            clean_map = {}
            for entry in self.pool:
                base = entry['symbol'].split('.')[0]
                if base not in clean_map or '.DE' in entry['symbol'] or '.' not in entry['symbol']:
                    clean_map[base] = entry
            self.pool = list(clean_map.values())
        else: self.pool = []

    def run(self):
        # Initial Dashboard
        inspector.log("SYSTEM", "START", f"V269 Jitter-Turbo | Pool: {len(self.pool)} | Blacklist: {len(self.blacklist)}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in range(0, len(self.pool), BATCH_SIZE):
                batch = self.pool[i:i+BATCH_SIZE]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                concurrent.futures.wait(futures)
                
                self.flush_buffer_to_disk()
                
                elapsed = (time.time() - inspector.stats['start']) / 60
                inspector.log("STATS", "DASH", f"Proc: {inspector.stats['processed']} | Speed: {inspector.stats['processed']/elapsed:.1f} Ast/Min")

if __name__ == "__main__":
    AureumSentinel().run()
