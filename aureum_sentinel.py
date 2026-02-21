import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
import threading
import random
from datetime import datetime

# --- SETTINGS ---
HERITAGE_ROOT = "heritage/"
LIVE_TICKER_FEATHER = "heritage/live_ticker.feather"
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
MAX_WORKERS = 20 # Wieder hochgefahren, da Multi-Level-Storage Schreiblast verteilt
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

    def expand_pool_via_wiki(self):
        """Erweitert den Pool organisch via Wikipedia (S&P 500, DAX, etc.)"""
        urls = [
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            "https://en.wikipedia.org/wiki/DAX",
            "https://en.wikipedia.org/wiki/NASDAQ-100"
        ]
        headers = {'User-Agent': 'Mozilla/5.0'}
        new_found = 0
        for url in urls:
            try:
                r = requests.get(url, headers=headers)
                tables = pd.read_html(io.StringIO(r.text))
                for df in tables:
                    for col in ['Symbol', 'Ticker', 'Ticker symbol']:
                        if col in df.columns:
                            for sym in df[col].astype(str).unique():
                                sym = sym.replace('.', '-') # Yahoo Format
                                if not any(a['symbol'] == sym for a in self.pool):
                                    self.pool.append({"symbol": sym, "last_sync": "1900-01-01"})
                                    new_found += 1
            except: continue
        print(f"[WIKI] {new_found} neue Assets gefunden. Pool-Größe: {len(self.pool)}")

    def fetch_task(self, asset):
        ticker = asset['symbol']
        stooq_ticker = ticker.upper() if "." in ticker else f"{ticker.upper()}.US"
        headers = {'User-Agent': f'Aureum-Bot-{random.randint(1,1000)}'}
        
        live_5m, gap_1d, hist_df = None, None, None
        price = 0.0
        
        try:
            # Yahoo
            stock = yf.Ticker(ticker)
            live_5m = stock.history(period="5d", interval="5m")
            gap_1d = stock.history(period="1mo", interval="1d")
            if not live_5m.empty: price = live_5m['Close'].iloc[-1]

            # Stooq
            r = requests.get(f"https://stooq.com/q/d/l/?s={stooq_ticker}&i=d", headers=headers, timeout=5)
            if len(r.content) > 300:
                hist_df = pd.read_csv(io.StringIO(r.text), index_col='Date', parse_dates=True)
        except: pass

        status = "FULL" if (hist_df is not None and price > 0) else "PARTIAL"
        return {"ticker": ticker, "price": price, "hist": hist_df, "live": live_5m, "gap": gap_1d, "status": status}

    def safe_store(self, res):
        if not res or res['price'] == 0: return
        with storage_lock:
            # 1. Live
            if res['live'] is not None:
                df_l = res['live'].copy()
                df_l['Ticker'] = res['ticker']
                self._write(df_l.reset_index(), LIVE_TICKER_FEATHER, "feather")

            # 2. Heritage (Decade/Year)
            if res['hist'] is not None:
                combined = pd.concat([res['hist'], res['gap'] or pd.DataFrame()]).sort_index()
                combined = combined[~combined.index.duplicated(keep='last')]
                combined['Year'] = combined.index.year
                combined['Decade'] = (combined['Year'] // 10) * 10
                
                for (dec, yr), group in combined.groupby(['Decade', 'Year']):
                    path = f"{HERITAGE_ROOT}{int(dec)}s/heritage_{int(yr)}.parquet"
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    g = group.copy(); g['Ticker'] = res['ticker']
                    self._write(g, path, "parquet")

            # Status Update
            for a in self.pool:
                if a['symbol'] == res['ticker']:
                    a['last_sync'] = datetime.now().isoformat()
                    break

    def _write(self, df, path, fmt):
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
        print(f"=== AUREUM SENTINEL V204 | 10k+ SYNC START [{datetime.now()}] ===")
        self.expand_pool_via_wiki()
        
        # Sortiere: Am längsten nicht synchronisierte zuerst
        self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
        
        # Verarbeite alles, was möglich ist (Full Batch)
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.fetch_task, a) for a in self.pool[:12000]]
            for f in concurrent.futures.as_completed(futures):
                res = f.result()
                if res:
                    self.safe_store(res)
                    if random.random() < 0.05: # Nur 5% loggen um Runner-Log sauber zu halten
                        print(f"[SYNC] {res['ticker']} - {res['status']}")

        with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)
        print(f"=== Zyklus beendet. Pool: {len(self.pool)} Assets ===")

if __name__ == "__main__":
    AureumSentinel().run()
