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

# --- KONFIGURATION ---
HERITAGE_ROOT = "heritage/"
LIVE_TICKER_FEATHER = "heritage/live_ticker.feather"
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
MAX_WORKERS = 15 # Reduziert auf 15, um Stooq-Blocking zu vermeiden
storage_lock = threading.Lock()

class AureumSentinel:
    def __init__(self):
        os.makedirs(HERITAGE_ROOT, exist_ok=True)
        self.load_pool()
        self.audit_logs = []

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = []

    def log(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] [{level:7}] {ticker:8} | {message}", flush=True)

    def fetch_task(self, asset):
        ticker = asset['symbol']
        self.log("START", ticker, "Initiiere Datensatz-Abruf...")
        
        # Stooq-Ticker Strategie
        stooq_variants = [ticker.upper(), f"{ticker.upper()}.US"] if "." not in ticker else [ticker.upper()]
        headers = {'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) {random.randint(100,999)}'}
        
        live_5m, gap_1d, hist_df = None, None, None
        price = 0.0
        stooq_status = "❌"
        
        try:
            # 1. Yahoo
            self.log("YAHOO", ticker, "Rufe 5m und Daily Gap-Fill ab...")
            stock = yf.Ticker(ticker)
            live_5m = stock.history(period="5d", interval="5m")
            gap_1d = stock.history(period="1mo", interval="1d")
            if not live_5m.empty:
                price = live_5m['Close'].iloc[-1]
                self.log("PRICE", ticker, f"Aktueller Kurs: {price:.2f}")

            # 2. Stooq mit Varianten-Check
            for s_ticker in stooq_variants:
                self.log("STOOQ", ticker, f"Versuche Heritage-Abruf mit '{s_ticker}'...")
                r = requests.get(f"https://stooq.com/q/d/l/?s={s_ticker}&i=d", headers=headers, timeout=5)
                if len(r.content) > 300:
                    hist_df = pd.read_csv(io.StringIO(r.text), index_col='Date', parse_dates=True)
                    stooq_status = "✅"
                    self.log("SUCCESS", ticker, "Stooq-Historie erfolgreich geladen.")
                    break
                time.sleep(0.2) # Kleine Pause gegen Blocking

        except Exception as e:
            self.log("ERROR", ticker, f"Fehler im Fetch: {str(e)}")

        status = "FULL" if (stooq_status == "✅" and price > 0) else "PARTIAL"
        return {"ticker": ticker, "price": price, "hist": hist_df, "live": live_5m, "gap": gap_1d, "status": status}

    def safe_store(self, res):
        if not res or res['price'] == 0: return
        ticker = res['ticker']
        
        with storage_lock:
            # A. Live Ticker
            if res['live'] is not None and not res['live'].empty:
                self.log("STORE", ticker, "Speichere Live-Ticker (Feather)...")
                df_l = res['live'].copy()
                df_l['Ticker'] = ticker
                self._atomic_save(df_l.reset_index(), LIVE_TICKER_FEATHER, "feather")

            # B. Heritage
            if res['hist'] is not None and not res['hist'].empty:
                self.log("MARRY", ticker, "Verheirate Stooq + Yahoo-Gap...")
                combined = pd.concat([res['hist'], res['gap'] or pd.DataFrame()]).sort_index()
                combined = combined[~combined.index.duplicated(keep='last')]
                combined['Year'] = combined.index.year
                combined['Decade'] = (combined['Year'] // 10) * 10
                
                for (dec, yr), group in combined.groupby(['Decade', 'Year']):
                    path = f"{HERITAGE_ROOT}{int(dec)}s/heritage_{int(yr)}.parquet"
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    g = group.copy(); g['Ticker'] = ticker
                    self.log("DISK", ticker, f"Schreibe Heritage {int(yr)} nach {path}")
                    self._atomic_save(g, path, "parquet")

            # Zeitstempel
            for a in self.pool:
                if a['symbol'] == ticker:
                    a['last_sync'] = datetime.now().isoformat()
                    break

    def _atomic_save(self, df, path, fmt):
        try:
            if os.path.exists(path):
                old = pd.read_parquet(path) if fmt == "parquet" else pd.read_feather(path)
                df = pd.concat([old, df])
                t_col = 'Date' if 'Date' in df.columns else ('Datetime' if 'Datetime' in df.columns else df.index.name)
                df = df.drop_duplicates(subset=[t_col, 'Ticker']) if t_col else df
            
            tmp = path + ".tmp"
            if fmt == "parquet": df.to_parquet(tmp, compression='snappy')
            else: df.to_feather(tmp)
            os.replace(tmp, path)
        except Exception as e:
            print(f"!!! CRITICAL DISK ERROR: {str(e)}")

    def run(self):
        print(f"=== AUREUM SENTINEL V206 | DEEP LOGGING START [{datetime.now()}] ===")
        # Sortierung & Batch (wir nehmen 300 pro Lauf für bessere Stabilität)
        self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
        batch = self.pool[:300]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.fetch_task, a) for a in batch]
            for f in concurrent.futures.as_completed(futures):
                self.safe_store(f.result())

        with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)
        print(f"=== Zyklus beendet. {len(batch)} Assets verarbeitet. ===")

if __name__ == "__main__":
    AureumSentinel().run()
