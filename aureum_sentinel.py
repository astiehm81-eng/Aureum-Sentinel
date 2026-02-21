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
AUDIT_FILE = "heritage/sentinel_audit.log"
MAX_WORKERS = 30 
STOOQ_LOCK = threading.Lock() 
ANCHOR_THRESHOLD = 0.0005
SKOOQ_URL = "https://stooq.com/q/d/l/?s={ticker}&i=d"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) Chrome/121.0.0.0 Safari/537.36"
]

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.stats = {"total": 0, "merges": 0, "stooq_hits": 0, "start": time.time()}

    def log(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        line = f"[{ts}] [{level:7}] {ticker:10} | {message}"
        print(line, flush=True)
        with self.log_lock:
            os.makedirs(os.path.dirname(AUDIT_FILE), exist_ok=True)
            with open(AUDIT_FILE, "a", encoding="utf-8") as f: f.write(line + "\n")

    def apply_iron_anchor(self, df):
        if df.empty: return df
        df = df.sort_values('Date').copy()
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        anchors = [df.iloc[0].to_dict()]
        for i in range(1, len(df)):
            if abs((df.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR_THRESHOLD:
                anchors.append(df.iloc[i].to_dict())
        return pd.DataFrame(anchors)

    def save_heritage(self, df, ticker):
        for year, group in df.groupby(df['Date'].dt.year):
            path = f"{HERITAGE_ROOT}{(int(year)//10)*10}s/heritage_{int(year)}.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if os.path.exists(path):
                old = pd.read_parquet(path)
                group = pd.concat([old, group]).drop_duplicates(subset=['Date']).sort_values('Date')
            group.to_parquet(path, index=False, compression='snappy')
            with self.stats_lock: self.stats["merges"] += 1

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = [{"symbol": "SAP.DE"}]

    def fetch_stooq_safe(self, ticker):
        with STOOQ_LOCK:
            time.sleep(random.uniform(2.5, 4.5)) 
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            try:
                r = requests.get(SKOOQ_URL.format(ticker=ticker), headers=headers, timeout=12)
                if r.status_code == 200 and len(r.content) > 300:
                    with inspector.stats_lock: inspector.stats["stooq_hits"] += 1
                    return pd.read_csv(io.StringIO(r.text), parse_dates=['Date'])
            except: pass
            return pd.DataFrame()

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            hist = self.fetch_stooq_safe(ticker)
            stock = yf.Ticker(ticker)
            recent = stock.history(period="7d", interval="5m").reset_index()
            recent.rename(columns={'Datetime': 'Date', 'Date': 'Date'}, inplace=True, errors='ignore')
            
            if hist.empty and recent.empty: return "EMPTY"

            combined = pd.concat([hist, recent], ignore_index=True)
            combined['Date'] = pd.to_datetime(combined['Date'], utc=True).dt.tz_localize(None)
            
            clean_df = inspector.apply_iron_anchor(combined)
            inspector.save_heritage(clean_df, ticker)
            
            inspector.log("HEAL", ticker, f"Erfolg ({len(clean_df)} Anker)")
            return "OK"
        except Exception as e:
            inspector.log("FAIL", ticker, f"Fehler: {str(e)[:40]}")
            return "ERR"

    def run(self):
        inspector.log("SYSTEM", "START", f"V246 gestartet. Pool: {len(self.pool)}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Batches für bessere Dashboard-Updates
            for i in range(0, len(self.pool), 300):
                batch = self.pool[i:i+300]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                for f in concurrent.futures.as_completed(futures):
                    with inspector.stats_lock: inspector.stats["total"] += 1
                    if inspector.stats["total"] % 30 == 0:
                        elapsed = (time.time() - inspector.stats["start"]) / 60
                        print(f"\n[DASHBOARD] Stooq: {inspector.stats['stooq_hits']} | Progress: {(inspector.stats['total']/len(self.pool))*100:.1f}% | Speed: {inspector.stats['total']/elapsed:.1f} Ast/Min\n")
                
                with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)

if __name__ == "__main__":
    AureumSentinel().run()
