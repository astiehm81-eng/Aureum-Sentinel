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
AUDIT_FILE = "heritage/heritage_audit.txt"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 30 
STOOQ_SEMAPHORE = threading.Semaphore(3) 

class AureumInspector:
    def __init__(self, root_path):
        self.root = root_path
        self.lock = threading.Lock()
        self.audit_lock = threading.Lock()
        self.stats = {"success": 0, "error": 0, "data_points": 0, "skips": 0}
        
        os.makedirs(self.root, exist_ok=True)
        self._write_audit(f"\n{'='*60}\nSESSION START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}\n")

    def _write_audit(self, message):
        with self.audit_lock:
            with open(AUDIT_FILE, "a", encoding="utf-8") as f:
                f.write(message)

    def log_event(self, level, ticker, message, to_file=True):
        ts = datetime.now().strftime('%H:%M:%S')
        log_line = f"[{ts}] [{level:9}] {ticker:8} | {message}"
        print(log_line, flush=True) # Konsole für GitHub Actions
        if to_file:
            self._write_audit(log_line + "\n")

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
                if 'Ticker' not in df.columns: df['Ticker'] = ticker
                
                path = os.path.join(self.root, filename)
                os.makedirs(os.path.dirname(path), exist_ok=True)

                if os.path.exists(path):
                    old = pd.read_parquet(path) if fmt=="parquet" else pd.read_feather(path)
                    if t_col in old.columns:
                        old[t_col] = pd.to_datetime(old[t_col], utc=True, errors='coerce').dt.tz_localize(None)
                    df = pd.concat([old, df]).drop_duplicates(subset=[t_col, 'Ticker'])

                if fmt == "parquet": df.to_parquet(path, compression='snappy', index=False)
                else: df.to_feather(path)
                
                self.stats["success"] += 1
                self.stats["data_points"] += len(df)
            except Exception as e:
                self.stats["error"] += 1
                self.log_event("DSK-ERR", ticker, f"Speicherfehler in {filename}: {e}")

inspector = AureumInspector(HERITAGE_ROOT)

class AureumSentinel:
    def __init__(self):
        self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: raw = json.load(f)
            for a in raw: a['symbol'] = a['symbol'].replace('.MC.MC', '.MC')
            self.pool = raw
            self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
            inspector.log_event("AUDIT", "SYSTEM", f"Pool geladen: {len(self.pool)} Assets.", to_file=False)
        else: self.pool = []

    def fetch_stooq(self, ticker):
        st_ticker = ticker if "." in ticker else f"{ticker.upper()}.US"
        with STOOQ_SEMAPHORE:
            time.sleep(random.uniform(0.07, 0.15))
            try:
                r = requests.get(f"https://stooq.com/q/d/l/?s={st_ticker}&i=d", timeout=5)
                if len(r.content) > 300:
                    df = pd.read_csv(io.StringIO(r.text), index_col='Date', parse_dates=True).reset_index()
                    inspector.log_event("STOOQ-OK", ticker, f"Heritage erhalten ({len(df)} Zeilen)")
                    return df
            except: pass
            return None

    def worker_task(self, asset):
        ticker = asset['symbol']
        inspector.log_event("START", ticker, "Sync initiiert")
        try:
            stock = yf.Ticker(ticker)
            live = stock.history(period="5d", interval="5m").reset_index()
            
            if live.empty:
                inspector.log_event("SKIP", ticker, "Yahoo liefert keine Daten (Delisted?)")
                inspector.stats["skips"] += 1
                return {"ticker": ticker, "status": "EMPTY"}

            inspector.log_event("YAHOO-OK", ticker, f"Live-Kurs: {live['Close'].iloc[-1]:.2f}")
            gap = stock.history(period="1mo", interval="1d").reset_index()
            hist = self.fetch_stooq(ticker)
            
            return {"ticker": ticker, "live": live, "gap": gap, "hist": hist, "status": "OK"}
        except Exception as e:
            inspector.log_event("ERROR", ticker, f"Systemfehler: {e}")
            return {"ticker": ticker, "status": "ERROR"}

    def orchestrate(self, res):
        ticker = res['ticker']
        for a in self.pool:
            if a['symbol'] == ticker:
                a['last_sync'] = datetime.now().isoformat()
                break
        if res['status'] != "OK": return

        inspector.save(res['live'], "live_ticker.feather", ticker, fmt="feather")
        if res['hist'] is not None:
            try:
                h, g = res['hist'].copy(), res['gap'].copy()
                h['Date'] = pd.to_datetime(h['Date'], utc=True).dt.tz_localize(None)
                g['Date'] = pd.to_datetime(g['Date'], utc=True).dt.tz_localize(None)
                combined = pd.concat([h, g]).drop_duplicates(subset=['Date'])
                for year, group in combined.groupby(combined['Date'].dt.year):
                    inspector.save(group, f"{(int(year)//10)*10}s/heritage_{int(year)}.parquet", ticker)
            except Exception as e:
                inspector.log_event("ORCH-ERR", ticker, f"Marriage failed: {e}")

    def run(self):
        start_time = time.time()
        inspector.log_event("SESSION", "START", "Turbo Auditor Pipeline aktiv.")
        
        batch = self.pool[:5000]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.worker_task, a) for a in batch]
            for f in concurrent.futures.as_completed(futures):
                self.orchestrate(f.result())

        with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)
        
        duration = (time.time() - start_time) / 60
        summary = (f"\n{'='*60}\nFINAL SUMMARY\n"
                   f"Dauer: {duration:.2f} Min\n"
                   f"Erfolge: {inspector.stats['success']} | Skips: {inspector.stats['skips']} | Fehler: {inspector.stats['error']}\n"
                   f"Datenpunkte gesamt: {inspector.stats['data_points']}\n{'='*60}\n")
        inspector._write_audit(summary)
        print(summary)

if __name__ == "__main__":
    AureumSentinel().run()
