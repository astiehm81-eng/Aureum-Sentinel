import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
import threading
import random
import time
from datetime import datetime

# --- KONFIGURATION TURBO MODE ---
HERITAGE_ROOT = "heritage/"
LIVE_TICKER_FEATHER = "heritage/live_ticker.feather"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 35 # Erhöht für I/O Waiting
STOOQ_SEMAPHORE = threading.Semaphore(3) # Erlaubt 3 parallele Stooq-Requests

class AureumInspector:
    def __init__(self, root_path):
        self.root = root_path
        self.lock = threading.Lock()
        self.stats = {"success": 0, "error": 0, "data_points": 0}

    def _normalize(self, df):
        t_col = 'Date' if 'Date' in df.columns else ('Datetime' if 'Datetime' in df.columns else None)
        if t_col:
            df[t_col] = pd.to_datetime(df[t_col], utc=True, errors='coerce').dt.tz_localize(None)
            df = df.dropna(subset=[t_col])
        return df, t_col

    def save(self, df, filename, ticker, fmt="parquet"):
        with self.lock:
            try:
                df, t_col = self._normalize(df.copy())
                if df.empty: return
                path = os.path.join(self.root, filename)
                os.makedirs(os.path.dirname(path), exist_ok=True)

                if os.path.exists(path):
                    old = pd.read_parquet(path) if fmt=="parquet" else pd.read_feather(path)
                    df = pd.concat([old, df]).drop_duplicates(subset=[t_col, 'Ticker' if 'Ticker' in old.columns else t_col])

                if fmt == "parquet": df.to_parquet(path, compression='snappy', index=False)
                else: df.to_feather(path)
                self.stats["success"] += 1
                self.stats["data_points"] += len(df)
            except Exception as e:
                self.stats["error"] += 1

inspector = AureumInspector(HERITAGE_ROOT)

class AureumSentinel:
    def __init__(self):
        self.pool_file = POOL_FILE
        self.load_pool()

    def log(self, level, ticker, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level:8}] {ticker:8} | {msg}", flush=True)

    def load_pool(self):
        if os.path.exists(self.pool_file):
            with open(self.pool_file, "r") as f: self.pool = json.load(f)
            self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
        else: self.pool = []

    def fetch_stooq(self, ticker):
        # Nutzt Semaphore statt Lock für kontrollierte Parallelität
        with STOOQ_SEMAPHORE:
            time.sleep(random.uniform(0.05, 0.1)) # Minimales Delay bleibt Pflicht
            st_ticker = ticker if "." in ticker else f"{ticker.upper()}.US"
            try:
                r = requests.get(f"https://stooq.com/q/d/l/?s={st_ticker}&i=d", timeout=5)
                if len(r.content) > 300:
                    return pd.read_csv(io.StringIO(r.text), index_col='Date', parse_dates=True).reset_index()
            except: pass
            return None

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # 1. Schneller Yahoo Check (Period 1d reicht oft für Status-Check)
            stock = yf.Ticker(ticker)
            live = stock.history(period="5d", interval="5m").reset_index()
            
            if live.empty:
                self.log("SKIP", ticker, "Delisted/Empty")
                return {"ticker": ticker, "status": "EMPTY"}

            # 2. Parallel dazu Stooq triggern
            self.log("FETCH", ticker, "Yahoo OK -> Syncing Heritage...")
            gap = stock.history(period="1mo", interval="1d").reset_index()
            hist = self.fetch_stooq(ticker)
            
            return {"ticker": ticker, "live": live, "gap": gap, "hist": hist, "status": "OK"}
        except Exception as e:
            self.log("ERROR", ticker, str(e))
            return {"ticker": ticker, "status": "ERROR"}

    def orchestrate(self, res):
        ticker = res['ticker']
        for a in self.pool:
            if a['symbol'] == ticker:
                a['last_sync'] = datetime.now().isoformat()
                break

        if res['status'] != "OK": return

        # Live Save
        inspector.save(res['live'], "live_ticker.feather", ticker, fmt="feather")

        # Heritage Merge
        if res['hist'] is not None:
            h, g = res['hist'].copy(), res['gap'].copy()
            h['Date'] = pd.to_datetime(h['Date'], utc=True).dt.tz_localize(None)
            g['Date'] = pd.to_datetime(g['Date'], utc=True).dt.tz_localize(None)
            combined = pd.concat([h, g]).drop_duplicates(subset=['Date'])
            
            for year, group in combined.groupby(combined['Date'].dt.year):
                inspector.save(group, f"{(int(year)//10)*10}s/heritage_{int(year)}.parquet", ticker)

    def run(self):
        start_time = time.time()
        print(f"=== AUREUM SENTINEL V221 | TURBO PIPELINE START ===")
        
        # Wir erhöhen das Batch-Fenster auf 10.000, da wir schneller sind
        batch = self.pool[:10000]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.worker_task, a) for a in batch]
            for f in concurrent.futures.as_completed(futures):
                self.orchestrate(f.result())

        with open(self.pool_file, "w") as f: json.dump(self.pool, f, indent=4)
        
        duration = (time.time() - start_time) / 60
        print(f"\n=== TURBO RUN BEENDET ===")
        print(f"Dauer: {duration:.2f} Min | Durchsatz: {len(batch)/duration:.1f} Assets/Min")
        print(f"Stats: {inspector.stats}")

if __name__ == "__main__":
    AureumSentinel().run()
