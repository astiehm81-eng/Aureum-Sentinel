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
storage_lock = threading.Lock()
stooq_throttle_lock = threading.Lock()

class AureumSentinel:
    def __init__(self):
        os.makedirs(HERITAGE_ROOT, exist_ok=True)
        self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
            self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
        else: self.pool = []

    def log(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] [{level:7}] {ticker:8} | {message}", flush=True)

    def fetch_stooq_with_delay(self, s_ticker):
        with stooq_throttle_lock:
            time.sleep(random.uniform(0.05, 0.15))
            headers = {'User-Agent': f'Mozilla/5.0 (Aureum-V210; {random.random()})'}
            try:
                r = requests.get(f"https://stooq.com/q/d/l/?s={s_ticker}&i=d", headers=headers, timeout=5)
                if len(r.content) > 300:
                    df = pd.read_csv(io.StringIO(r.text), index_col='Date', parse_dates=True)
                    # TZ-Fix: Stooq ist meist naive, aber sicherheitshalber strippen
                    if df.index.tz is not None: df.index = df.index.tz_localize(None)
                    return df
            except: pass
            return None

    def fetch_task(self, asset):
        ticker = asset['symbol']
        self.log("START", ticker, "Batch-Abruf...")
        live_5m, gap_1d, hist_df = None, None, None
        price = 0.0
        
        try:
            # 1. Yahoo (mit TZ-Stripping)
            stock = yf.Ticker(ticker)
            live_5m = stock.history(period="5d", interval="5m")
            gap_1d = stock.history(period="1mo", interval="1d")
            
            if not live_5m.empty:
                live_5m.index = live_5m.index.tz_localize(None)
                price = live_5m['Close'].iloc[-1]
                self.log("YAHOO", ticker, f"P: {price:.2f}")
            
            if not gap_1d.empty:
                gap_1d.index = gap_1d.index.tz_localize(None)

            # 2. Stooq
            st_ticker = ticker.upper() if "." in ticker else f"{ticker.upper()}.US"
            hist_df = self.fetch_stooq_with_delay(st_ticker)
            if hist_df is not None:
                self.log("STOOQ", ticker, "Historie ✅")

        except Exception as e:
            self.log("ERROR", ticker, f"Fehler: {str(e)}")

        return {"ticker": ticker, "price": price, "hist": hist_df, "live": live_5m, "gap": gap_1d}

    def safe_store(self, res):
        if not res or res['price'] == 0: return
        ticker = res['ticker']
        
        with storage_lock:
            # A. Live Ticker
            if res['live'] is not None and not res['live'].empty:
                df_l = res['live'].copy(); df_l['Ticker'] = ticker
                self._atomic_save(df_l.reset_index(), LIVE_TICKER_FEATHER, "feather")

            # B. Heritage (Marriage)
            if res['hist'] is not None and not res['hist'].empty:
                dfs = [res['hist']]
                if res['gap'] is not None and not res['gap'].empty:
                    dfs.append(res['gap'])
                
                combined = pd.concat(dfs).sort_index()
                combined = combined[~combined.index.duplicated(keep='last')]
                combined['Year'] = combined.index.year
                combined['Decade'] = (combined['Year'] // 10) * 10
                
                for (dec, yr), group in combined.groupby(['Decade', 'Year']):
                    path = f"{HERITAGE_ROOT}{int(dec)}s/heritage_{int(yr)}.parquet"
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    g = group.copy(); g['Ticker'] = ticker
                    self._atomic_save(g, path, "parquet")

            # Sync-Markierung
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
        except: pass

    def run(self):
        print(f"=== AUREUM SENTINEL V210 | TZ-SAFE 10k SYNC START [{datetime.now()}] ===")
        batch = self.pool[:10000] # Ziel: Volle 10k
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.fetch_task, a) for a in batch]
            for f in concurrent.futures.as_completed(futures):
                try:
                    self.safe_store(f.result())
                except Exception as e:
                    print(f"Storage Error [{datetime.now()}]: {e}")

        with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)
        print(f"=== Zyklus beendet. Pool aktualisiert. ===")

if __name__ == "__main__":
    AureumSentinel().run()
