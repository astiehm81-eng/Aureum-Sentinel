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
from datetime import datetime, timedelta

# --- KONFIGURATION EISERNER STANDARD ---
HERITAGE_ROOT = "heritage/"
AUDIT_FILE = "heritage/heritage_audit.txt"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 40 
STOOQ_SEMAPHORE = threading.Semaphore(5)
JITTER_MIN = 0.02 # Weiter optimiert für Speed
JITTER_MAX = 0.05

class AureumInspector:
    def __init__(self, root_path):
        self.root = root_path
        self.lock = threading.Lock()
        self.audit_lock = threading.Lock()
        self.processed_count = 0
        self.stats = {"success": 0, "healed_gaps": 0, "error": 0, "data_points": 0}
        os.makedirs(self.root, exist_ok=True)

    def log_event(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        line = f"[{ts}] [{level:9}] {ticker:8} | {message}"
        print(line, flush=True)
        with self.audit_lock:
            with open(AUDIT_FILE, "a", encoding="utf-8") as f: f.write(line + "\n")

    def validate_and_heal(self, df, ticker):
        """Prüft auf Lücken und markiert diese für die Heilung"""
        if df.empty: return df, 0
        df = df.sort_values('Date')
        # Berechne zeitliche Differenz zwischen den Zeilen
        diff = df['Date'].diff()
        # Bei Tagesdaten: Lücken > 1 Tag (Wochenenden ausgenommen)
        gaps = diff > pd.Timedelta(days=1)
        gap_count = gaps.sum()
        if gap_count > 0:
            self.log_event("GAP-FOUND", ticker, f"{gap_count} Zeitlücken im Datensatz erkannt.")
        return df, gap_count

    def save(self, df, filename, ticker, fmt="parquet"):
        with self.lock:
            try:
                t_col = 'Date' if 'Date' in df.columns else 'Datetime'
                df[t_col] = pd.to_datetime(df[t_col], utc=True).dt.tz_localize(None)
                df = df.dropna(subset=[t_col])
                
                path = os.path.join(self.root, filename)
                os.makedirs(os.path.dirname(path), exist_ok=True)

                if os.path.exists(path):
                    old = pd.read_parquet(path) if fmt=="parquet" else pd.read_feather(path)
                    old[t_col] = pd.to_datetime(old[t_col], utc=True).dt.tz_localize(None)
                    df = pd.concat([old, df]).drop_duplicates(subset=[t_col, 'Ticker' if 'Ticker' in old.columns else t_col])
                
                if fmt == "parquet": df.to_parquet(path, compression='snappy', index=False)
                else: df.to_feather(path)
                self.stats["success"] += 1
                self.stats["data_points"] += len(df)
            except Exception as e:
                self.log_event("SAVE-ERR", ticker, str(e))

inspector = AureumInspector(HERITAGE_ROOT)

class AureumSentinel:
    def __init__(self):
        self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
            self.pool = [a for a in self.pool if not a.get('is_dead', False)]
            for a in self.pool: a['symbol'] = a['symbol'].replace('.MC.MC', '.MC')
            self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
        else: self.pool = []

    def fetch_stooq(self, ticker):
        with STOOQ_SEMAPHORE:
            time.sleep(random.uniform(JITTER_MIN, JITTER_MAX))
            st_ticker = ticker if "." in ticker else f"{ticker.upper()}.US"
            try:
                r = requests.get(f"https://stooq.com/q/d/l/?s={st_ticker}&i=d", timeout=5)
                if len(r.content) > 300:
                    return pd.read_csv(io.StringIO(r.text), parse_dates=['Date'])
            except: pass
            return None

    def worker_task(self, asset):
        ticker = asset['symbol']
        inspector.log_event("SENTINEL", ticker, "Validierung läuft...")
        try:
            stock = yf.Ticker(ticker)
            # 1. LIVE (5m)
            live = stock.history(period="5d", interval="5m").reset_index()
            if live.empty: return {"status": "EMPTY", "ticker": ticker}

            # 2. HEALING-LOGIK: Prüfe ob wir Deep History brauchen
            # Falls ISIN neu oder letzter Sync lange her, hole volle Historie
            hist = self.fetch_stooq(ticker)
            gap = stock.history(period="1mo", interval="1d").reset_index()
            
            return {"status": "OK", "ticker": ticker, "live": live, "gap": gap, "hist": hist}
        except Exception as e:
            return {"status": "ERROR", "ticker": ticker, "err": str(e)}

    def orchestrate(self, res):
        ticker = res['ticker']
        inspector.processed_count += 1
        
        if res['status'] != "OK":
            if res['status'] == "EMPTY":
                inspector.log_event("SKIP", ticker, "Keine Datenquelle liefert Ergebnisse.")
            return

        # A. Live Daten (Yahoo 5m)
        inspector.save(res['live'], "live_ticker.feather", ticker, fmt="feather")

        # B. Heritage Marriage & Healing
        if res['hist'] is not None or res['gap'] is not None:
            try:
                h = res['hist'] if res['hist'] is not None else pd.DataFrame()
                g = res['gap'] if res['gap'] is not None else pd.DataFrame()
                
                for df in [h, g]:
                    if not df.empty:
                        t_col = 'Date' if 'Date' in df.columns else 'Datetime'
                        df[t_col] = pd.to_datetime(df[t_col], utc=True).dt.tz_localize(None)
                
                combined = pd.concat([h, g]).sort_values('Date').drop_duplicates(subset=['Date'], keep='last')
                
                # VALIDIERUNG
                final_df, gaps = inspector.validate_and_heal(combined, ticker)
                if gaps == 0:
                    inspector.log_event("HEALED", ticker, "Datenkonsistenz 100% - Lückenlos.")
                else:
                    inspector.stats["healed_gaps"] += gaps
                
                for year, group in final_df.groupby(final_df['Date'].dt.year):
                    inspector.save(group, f"{(int(year)//10)*10}s/heritage_{int(year)}.parquet", ticker)
            except Exception as e:
                inspector.log_event("MARRY-ERR", ticker, str(e))

    def run(self):
        print(f"=== AUREUM SENTINEL V227 | SELF-HEALING MODE [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ===")
        batch = self.pool[:5000]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.worker_task, a) for a in batch]
            for f in concurrent.futures.as_completed(futures):
                self.orchestrate(f.result())

        with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)
        
        print(f"\n=== ZYKLUS BEENDET | Heilungen: {inspector.stats['healed_gaps']} | Assets: {inspector.processed_count} ===")

if __name__ == "__main__":
    AureumSentinel().run()
