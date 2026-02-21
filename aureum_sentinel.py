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

# --- KONFIGURATION NITRO ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
MAX_WORKERS_YAHOO = 50 # Erhöht für parallele Vorlast
STOOQ_LOCK = threading.Lock() 
STORAGE_LOCK = threading.Lock() 
ANCHOR_THRESHOLD = 0.0005 

# Erweiterte Liste für bessere Streuung bei hoher Frequenz
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0"
]

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats = {"processed": 0, "stooq_calls": 0, "start": time.time()}

    def log(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] [{level:7}] {ticker:10} | {message}", flush=True)

    def normalize_dates(self, df):
        if df.empty: return df
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        return df

    def get_partition_path(self, ticker, year):
        first_char = ticker[0].upper() if ticker[0].isalpha() else "_"
        return f"{HERITAGE_ROOT}{(int(year)//10)*10}s/{first_char}_{int(year)}.parquet"

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = [{"symbol": "SAP.DE"}]

    def fetch_stooq_nitro(self, ticker):
        """DER SWEET SPOT TEST: Reduziert auf 0.4s - 0.9s"""
        with STOOQ_LOCK:
            # Wir tasten uns an die 100-200ms ran, starten aber hier:
            time.sleep(random.uniform(0.4, 0.9)) 
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            try:
                r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", headers=headers, timeout=8)
                if r.status_code == 200 and len(r.content) > 300:
                    with inspector.stats_lock: inspector.stats["stooq_calls"] += 1
                    return pd.read_csv(io.StringIO(r.text), parse_dates=['Date'])
                elif r.status_code == 403:
                    inspector.log("ALERT", ticker, "RATE LIMIT! Erhöhe Pause kurzzeitig...")
                    time.sleep(5)
            except: pass
            return pd.DataFrame()

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # 1. Schneller Vorab-Check (Yahoo läuft immer parallel)
            stock = yf.Ticker(ticker)
            recent = stock.history(period="5d", interval="5m").reset_index()
            recent.rename(columns={'Datetime': 'Date'}, inplace=True, errors='ignore')
            
            # 2. Stooq Nitro Abfrage
            hist = self.fetch_stooq_nitro(ticker)

            # 3. Heirat & Eiserner Standard
            combined = pd.concat([inspector.normalize_dates(hist), 
                                  inspector.normalize_dates(recent)], ignore_index=True)
            
            # 0,05% Anker Logik
            combined = combined.sort_values('Date').drop_duplicates(subset=['Date'])
            if combined.empty: return "EMPTY"
            
            anchors = [combined.iloc[0].to_dict()]
            for i in range(1, len(combined)):
                if abs((combined.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR_THRESHOLD:
                    anchors.append(combined.iloc[i].to_dict())
            clean_df = pd.DataFrame(anchors)

            # 4. Atomares Speichern in A-Z Partitionen
            with STORAGE_LOCK:
                for year, group in clean_df.groupby(clean_df['Date'].dt.year):
                    path = inspector.get_partition_path(ticker, year)
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    if os.path.exists(path):
                        old = pd.read_parquet(path)
                        group = pd.concat([old, group]).drop_duplicates(subset=['Date']).sort_values('Date')
                    group.to_parquet(path, index=False, compression='snappy')

            inspector.log("DONE", ticker, f"Speed-Sync OK ({len(clean_df)} Anker)")
            with inspector.stats_lock: inspector.stats["processed"] += 1
            return "OK"
        except Exception as e:
            inspector.log("FAIL", ticker, str(e)[:50])
            return "ERR"

    def run(self):
        inspector.log("SYSTEM", "START", f"V254 NITRO | Pool: {len(self.pool)} | Target: High-Freq")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_YAHOO) as executor:
            for i in range(0, len(self.pool), 500):
                batch = self.pool[i:i+500]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                for f in concurrent.futures.as_completed(futures):
                    pass
                
                # Speed-Check Dashboard
                elapsed = (time.time() - inspector.stats['start']) / 60
                speed = inspector.stats['processed'] / elapsed
                print(f"\n[NITRO-DASH] Speed: {speed:.1f} Ast/Min | Stooq-Total: {inspector.stats['stooq_calls']}\n")

if __name__ == "__main__":
    AureumSentinel().run()
