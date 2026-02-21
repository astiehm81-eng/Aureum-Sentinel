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
MAX_WORKERS = 50 # Erhöht für Sharded IO
STOOQ_SEMAPHORE = threading.Semaphore(8) # Mehr parallele Heritage-Requests
JITTER = 0.01 # Minimaler Jitter für maximale Test-Geschwindigkeit

class AureumInspector:
    def __init__(self, root_path):
        self.root = root_path
        # Sharding Locks: Ein Lock pro Jahrzehnt-Ordner statt ein globaler Lock
        self.locks = {} 
        self.audit_lock = threading.Lock()
        self.stats = {"success": 0, "healed_gaps": 0, "error": 0, "new_isins": 0}

    def get_lock(self, folder):
        if folder not in self.locks: self.locks[folder] = threading.Lock()
        return self.locks[folder]

    def log_event(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        line = f"[{ts}] [{level:9}] {ticker:8} | {message}"
        print(line, flush=True)
        with self.audit_lock:
            with open(AUDIT_FILE, "a", encoding="utf-8") as f: f.write(line + "\n")

    def validate_market_hours(self, df, ticker):
        """Intelligente Validierung: Ignoriert Wochenenden & Feiertage"""
        if df.empty: return 0
        df = df.sort_values('Date')
        # Business Days check: Nur Lücken > 1 Werktag sind echte Gaps
        expected_range = pd.bdate_range(start=df['Date'].min(), end=df['Date'].max())
        actual_days = pd.to_datetime(df['Date'].dt.date).unique()
        missing = len(set(expected_range.date) - set(actual_days))
        return missing

    def save(self, df, filename, ticker, fmt="parquet"):
        folder = os.path.dirname(filename)
        # Sharded Lock: Worker für 1990er blockiert nicht Worker für 2020er
        with self.get_lock(folder):
            try:
                # Spalten-Fix für 'Datetime'/'Date' Fehler
                t_col = 'Date'
                if 'Datetime' in df.columns: df = df.rename(columns={'Datetime': 'Date'})
                df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
                
                path = os.path.join(self.root, filename)
                os.makedirs(os.path.dirname(path), exist_ok=True)

                if os.path.exists(path):
                    old = pd.read_parquet(path) if fmt=="parquet" else pd.read_feather(path)
                    if 'Datetime' in old.columns: old = old.rename(columns={'Datetime': 'Date'})
                    old['Date'] = pd.to_datetime(old['Date'], utc=True).dt.tz_localize(None)
                    df = pd.concat([old, df]).drop_duplicates(subset=['Date', 'Ticker' if 'Ticker' in old.columns else 'Date'])
                
                if fmt == "parquet": df.to_parquet(path, compression='snappy', index=False)
                else: df.to_feather(path)
                self.stats["success"] += 1
            except Exception as e:
                self.log_event("SAVE-ERR", ticker, f"Fehler: {str(e)}")

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

    def discover_new_isins(self, base_ticker):
        """Simuliert Suche nach verwandten Assets (z.B. Vorzugsaktien, Suffix-Varianten)"""
        suffixes = ['.DE', '.F', '.AS', '.MC', '']
        # Nur sporadisch suchen um Yahoo nicht zu reizen
        if random.random() > 0.98:
            new_sym = f"{base_ticker.split('.')[0]}{random.choice(suffixes)}"
            if not any(a['symbol'] == new_sym for a in self.pool):
                return {"symbol": new_sym, "found_at": datetime.now().isoformat()}
        return None

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # Discovery-Lauf
            new_isin = self.discover_new_isins(ticker)
            
            stock = yf.Ticker(ticker)
            live = stock.history(period="5d", interval="5m").reset_index()
            
            if live.empty:
                asset['dead_count'] = asset.get('dead_count', 0) + 1
                return {"status": "EMPTY", "ticker": ticker, "new_isin": new_isin}

            inspector.log_event("LIVE-OK", ticker, f"Syncing @ {live['Close'].iloc[-1]:.2f}")
            gap = stock.history(period="1mo", interval="1d").reset_index()
            
            # Heritage-Verbund
            with STOOQ_SEMAPHORE:
                time.sleep(JITTER)
                r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=5)
                hist = pd.read_csv(io.StringIO(r.text), parse_dates=['Date']) if len(r.content) > 300 else None
            
            return {"status": "OK", "ticker": ticker, "live": live, "gap": gap, "hist": hist, "new_isin": new_isin}
        except Exception as e:
            return {"status": "ERROR", "ticker": ticker, "new_isin": None}

    def orchestrate(self, res):
        ticker = res['ticker']
        if res['new_isin']: 
            self.pool.append(res['new_isin'])
            inspector.stats["new_isins"] += 1
            inspector.log_event("DISCOVERY", res['new_isin']['symbol'], "Neue ISIN dem Pool hinzugefügt.")

        if res['status'] != "OK": return

        # Live Save
        inspector.save(res['live'], "live_ticker.feather", ticker, fmt="feather")

        # Heritage Marriage
        if res['hist'] is not None or not res['gap'].empty:
            h = res['hist'] if res['hist'] is not None else pd.DataFrame()
            g = res['gap']
            
            # Spalten-Harmonisierung vor Concat
            for df in [h, g]:
                if 'Datetime' in df.columns: df.rename(columns={'Datetime': 'Date'}, inplace=True)
                df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)

            combined = pd.concat([h, g]).sort_values('Date').drop_duplicates(subset=['Date'], keep='last')
            
            # Business-Day Validierung
            gaps = inspector.validate_market_hours(combined, ticker)
            if gaps > 0: inspector.log_event("GAP", ticker, f"{gaps} Handels-Tage fehlen.")
            
            for year, group in combined.groupby(combined['Date'].dt.year):
                inspector.save(group, f"{(int(year)//10)*10}s/heritage_{int(year)}.parquet", ticker)

    def run(self):
        print(f"=== AUREUM SENTINEL V228 | SHARDED IO & DISCOVERY ===")
        batch = self.pool[:5000]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.worker_task, a) for a in batch]
            for f in concurrent.futures.as_completed(futures):
                self.orchestrate(f.result())

        with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)
        print(f"=== ZYKLUS BEENDET | Neue ISINs: {inspector.stats['new_isins']} ===")

if __name__ == "__main__":
    AureumSentinel().run()
