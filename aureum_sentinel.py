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

# --- KONFIGURATION EISERNER STANDARD ---
HERITAGE_ROOT = "heritage/"
LIVE_TICKER_FEATHER = "heritage/live_ticker.feather"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 15

class AureumInspector:
    """Zentraler Wächter: Erzwingt UTC-Safe Normalisierung"""
    def __init__(self, root_path):
        self.root = root_path
        self.lock = threading.Lock()
        self.stats = {"success": 0, "error": 0, "normalized": 0, "new_assets": 0}

    def log_disk(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] [{level:7}] {ticker:8} | {message}", flush=True)

    def _normalize(self, df):
        """Kritischer Fix: Konvertiert alles sicher zu TZ-Naive"""
        t_col = 'Date' if 'Date' in df.columns else ('Datetime' if 'Datetime' in df.columns else None)
        if t_col:
            # Fix für ValueError: utc=True erlaubt Konvertierung von tz-aware zu naive
            df[t_col] = pd.to_datetime(df[t_col], utc=True, errors='coerce').dt.tz_localize(None)
            df = df.dropna(subset=[t_col])
        return df, t_col

    def save(self, df, filename, ticker, fmt="parquet"):
        with self.lock:
            try:
                df, t_col = self._normalize(df.copy())
                if df.empty: return
                if 'Ticker' not in df.columns: df['Ticker'] = ticker
                
                path = os.path.join(self.root, filename)
                os.makedirs(os.path.dirname(path), exist_ok=True)

                if os.path.exists(path):
                    old = pd.read_parquet(path) if fmt=="parquet" else pd.read_feather(path)
                    # Inspector-Heilung für Altdaten
                    if t_col in old.columns:
                        old[t_col] = pd.to_datetime(old[t_col], utc=True, errors='coerce').dt.tz_localize(None)
                    df = pd.concat([old, df]).drop_duplicates(subset=[t_col, 'Ticker'])

                if fmt == "parquet": 
                    df.to_parquet(path, compression='snappy', index=False)
                else: 
                    df.to_feather(path)
                self.stats["success"] += 1
            except Exception as e:
                self.stats["error"] += 1
                self.log_disk("DSK-ERR", ticker, f"Speicherfehler: {e}")

class AureumSentinel:
    def __init__(self):
        self.pool_file = POOL_FILE
        self.inspector = AureumInspector(HERITAGE_ROOT)
        self.load_pool()
        self.stooq_lock = threading.Lock()

    def log_process(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] [{level:7}] {ticker:8} | {message}", flush=True)

    def load_pool(self):
        if os.path.exists(self.pool_file):
            with open(self.pool_file, "r") as f: self.pool = json.load(f)
            self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
        else: self.pool = []

    def fetch_stooq(self, ticker):
        # Ticker-Cleanup für Stooq (.MC.MC -> .MC)
        clean_ticker = ticker.split('.')[0]
        suffix = ticker.split('.')[-1]
        st_ticker = f"{clean_ticker}.{suffix}" if "." in ticker else f"{ticker.upper()}.US"
        
        with self.stooq_lock:
            time.sleep(random.uniform(0.06, 0.16))
            try:
                r = requests.get(f"https://stooq.com/q/d/l/?s={st_ticker}&i=d", timeout=5)
                if len(r.content) > 300:
                    df = pd.read_csv(io.StringIO(r.text), index_col='Date', parse_dates=True).reset_index()
                    self.log_process("STOOQ", ticker, "Heritage ok ✅")
                    return df
            except: pass
            return None

    def worker_task(self, asset):
        raw_ticker = asset['symbol']
        # Ticker-Bereinigung (.MC.MC -> .MC)
        ticker = raw_ticker
        if ticker.count('.MC') > 1: ticker = ticker.replace('.MC.MC', '.MC')
        
        is_new = " [NEW]" if 'last_sync' not in asset else ""
        self.log_process("START", ticker, f"Sync initiiert{is_new}")
        
        try:
            y_stock = yf.Ticker(ticker)
            live = y_stock.history(period="5d", interval="5m").reset_index()
            
            if live.empty:
                self.log_process("SKIP", ticker, "Yahoo EMPTY")
                return {"ticker": ticker, "status": "EMPTY"}

            self.log_process("YAHOO", ticker, f"Kurs: {live['Close'].iloc[-1]:.2f}")
            gap = y_stock.history(period="1mo", interval="1d").reset_index()
            hist = self.fetch_stooq(ticker)
            
            return {"ticker": ticker, "live": live, "gap": gap, "hist": hist, "status": "OK"}
        except Exception as e:
            self.log_process("ERROR", ticker, f"Worker-Fehler: {str(e)}")
            return {"ticker": ticker, "status": "ERROR"}

    def orchestrate(self, res):
        ticker = res['ticker']
        for a in self.pool:
            if a['symbol'] == ticker or a['symbol'].replace('.MC.MC', '.MC') == ticker:
                a['last_sync'] = datetime.now().isoformat()
                break

        if res['status'] != "OK": return

        # Live Speicher
        self.inspector.save(res['live'], "live_ticker.feather", ticker, fmt="feather")

        # Heritage Marriage mit TZ-Safety
        if res['hist'] is not None:
            try:
                # Hier lag der Absturz: Wir erzwingen utc=True vor dem Concat
                res['hist']['Date'] = pd.to_datetime(res['hist']['Date'], utc=True).dt.tz_localize(None)
                res['gap']['Date'] = pd.to_datetime(res['gap']['Date'], utc=True).dt.tz_localize(None)
                
                combined = pd.concat([res['hist'], res['gap']])
                for year, group in combined.groupby(combined['Date'].dt.year):
                    decade = (int(year) // 10) * 10
                    self.inspector.save(group, f"{decade}s/heritage_{int(year)}.parquet", ticker)
            except Exception as e:
                self.log_process("ORCH-ERR", ticker, f"Marriage failed: {e}")

    def run(self):
        print(f"=== AUREUM SENTINEL V218 | TZ-SAFE & TICKER-FIX [{datetime.now()}] ===")
        batch = self.pool[:5000]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.worker_task, a) for a in batch]
            for f in concurrent.futures.as_completed(futures):
                try:
                    self.orchestrate(f.result())
                except Exception as e:
                    print(f"CRITICAL ORCHESTRATION ERROR: {e}")

        with open(self.pool_file, "w") as f: json.dump(self.pool, f, indent=4)
        print(f"=== ZYKLUS BEENDET | Stats: {self.inspector.stats} ===")

if __name__ == "__main__":
    AureumSentinel().run()
