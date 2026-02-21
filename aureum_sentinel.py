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
from abc import ABC, abstractmethod

# --- INTERFACES & PATTERNS ---

class DataSource(ABC):
    """Strategy Pattern für Datenquellen"""
    @abstractmethod
    def fetch(self, ticker): pass

class AureumInspector:
    """Repository Pattern: Zentraler Wächter über die Festplatte"""
    def __init__(self, root_path):
        self.root = root_path
        self.lock = threading.Lock()
        self.stats = {"success": 0, "error": 0, "normalized": 0}

    def _normalize(self, df):
        """Erzwingt den Eisernen Standard für Zeitstempel"""
        t_col = 'Date' if 'Date' in df.columns else ('Datetime' if 'Datetime' in df.columns else None)
        if t_col:
            df[t_col] = pd.to_datetime(df[t_col], errors='coerce').dt.tz_localize(None)
            df = df.dropna(subset=[t_col])
        return df, t_col

    def save(self, df, filename, ticker, fmt="parquet"):
        with self.lock:
            try:
                df, t_col = self._normalize(df.copy())
                if 'Ticker' not in df.columns: df['Ticker'] = ticker
                
                path = os.path.join(self.root, filename)
                os.makedirs(os.path.dirname(path), exist_ok=True)

                if os.path.exists(path):
                    old = pd.read_parquet(path) if fmt=="parquet" else pd.read_feather(path)
                    # On-the-fly Heilung bestehender Daten
                    if t_col in old.columns and old[t_col].dtype == 'object':
                        old[t_col] = pd.to_datetime(old[t_col], errors='coerce').dt.tz_localize(None)
                        self.stats["normalized"] += 1
                    df = pd.concat([old, df]).drop_duplicates(subset=[t_col, 'Ticker'])

                if fmt == "parquet": df.to_parquet(path, compression='snappy', index=False)
                else: df.to_feather(path)
                self.stats["success"] += 1
            except Exception as e:
                self.stats["error"] += 1
                print(f"[!] INSPECTOR CRITICAL: {ticker} @ {filename} -> {e}")

# --- IMPLEMENTIERUNG ---

class AureumSentinel:
    def __init__(self):
        self.pool_file = "isin_pool.json"
        self.inspector = AureumInspector("heritage/")
        self.live_ticker_path = "heritage/live_ticker.feather"
        self.load_pool()
        self.stooq_lock = threading.Lock() # Throttler

    def load_pool(self):
        if os.path.exists(self.pool_file):
            with open(self.pool_file, "r") as f: self.pool = json.load(f)
            self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
        else: self.pool = []

    def fetch_stooq(self, ticker):
        """Stooq Abruf mit integriertem Pattern-Delay"""
        with self.stooq_lock:
            time.sleep(random.uniform(0.06, 0.16)) # Die gewünschten ~50ms+
            st_ticker = ticker.upper() if "." in ticker else f"{ticker.upper()}.US"
            try:
                r = requests.get(f"https://stooq.com/q/d/l/?s={st_ticker}&i=d", timeout=5)
                if len(r.content) > 300:
                    return pd.read_csv(io.StringIO(r.text), index_col='Date', parse_dates=True).reset_index()
            except: pass
            return None

    def worker_task(self, asset):
        ticker = asset['symbol']
        print(f"[{datetime.now().strftime('%H:%M:%S')}] SYNCing: {ticker}")
        
        try:
            # 1. Yahoo (Live & Gap)
            y_stock = yf.Ticker(ticker)
            live = y_stock.history(period="5d", interval="5m").reset_index()
            gap = y_stock.history(period="1mo", interval="1d").reset_index()
            
            if live.empty: return {"ticker": ticker, "status": "EMPTY"}

            # 2. Stooq (Heritage)
            hist = self.fetch_stooq(ticker)
            
            return {"ticker": ticker, "live": live, "gap": gap, "hist": hist, "status": "OK"}
        except Exception as e:
            return {"ticker": ticker, "status": "ERROR", "msg": str(e)}

    def orchestrate(self, res):
        ticker = res['ticker']
        # Pool-Update (Metadaten-Pattern)
        for a in self.pool:
            if a['symbol'] == ticker:
                a['last_sync'] = datetime.now().isoformat()
                break

        if res['status'] != "OK": return

        # Live-Daten Speicher-Logik
        self.inspector.save(res['live'], "live_ticker.feather", ticker, fmt="feather")

        # Heritage Marriage Logik
        if res['hist'] is not None:
            # Kombiniere Stooq (hist) und Yahoo (gap)
            combined = pd.concat([res['hist'], res['gap']])
            combined['Date'] = pd.to_datetime(combined['Date'])
            
            for year, group in combined.groupby(combined['Date'].dt.year):
                decade = (int(year) // 10) * 10
                filename = f"{decade}s/heritage_{int(year)}.parquet"
                self.inspector.save(group, filename, ticker)

    def run(self, batch_size=5000):
        print(f"=== AUREUM SENTINEL V215 | PATTERN ARCHITECTURE START ===")
        batch = self.pool[:batch_size]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(self.worker_task, a) for a in batch]
            for f in concurrent.futures.as_completed(futures):
                self.orchestrate(f.result())

        with open(self.pool_file, "w") as f: json.dump(self.pool, f, indent=4)
        print(f"=== ZYKLUS BEENDET | INSPECTOR STATS: {self.inspector.stats} ===")

if __name__ == "__main__":
    AureumSentinel().run()
