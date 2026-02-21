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

# --- KONFIGURATION EISERNER STANDARD ---
HERITAGE_ROOT = "heritage/"
LIVE_TICKER_FEATHER = "heritage/live_ticker.feather"
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
MAX_WORKERS = 20 
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
        """Sammelt ISINs/Ticker von Wikipedia (S&P 500, DAX, NASDAQ-100)."""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Starte Wiki-Expansion...")
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
                                sym = sym.replace('.', '-')
                                if not any(a['symbol'] == sym for a in self.pool):
                                    self.pool.append({"symbol": sym, "last_sync": "1900-01-01"})
                                    new_found += 1
            except: continue
        print(f"-> Wiki-Expansion abgeschlossen: {new_found} neue Ticker.")

    def fetch_task(self, asset):
        ticker = asset['symbol']
        # Stooq braucht für US-Werte oft das Suffix
        stooq_ticker = ticker.upper() if "." in ticker else f"{ticker.upper()}.US"
        headers = {'User-Agent': f'Aureum-Bot-{random.randint(1,1000)}'}
        
        live_5m, gap_1d, hist_df = None, None, None
        price = 0.0
        stooq_status = "❌"
        
        try:
            # 1. Yahoo (Live & Gap-Fill)
            stock = yf.Ticker(ticker)
            live_5m = stock.history(period="5d", interval="5m")
            gap_1d = stock.history(period="1mo", interval="1d")
            if not live_5m.empty: price = live_5m['Close'].iloc[-1]

            # 2. Stooq (Heritage)
            r = requests.get(f"https://stooq.com/q/d/l/?s={stooq_ticker}&i=d", headers=headers, timeout=5)
            if len(r.content) > 300:
                hist_df = pd.read_csv(io.StringIO(r.text), index_col='Date', parse_dates=True)
                stooq_status = "✅"
        except: pass

        status = "FULL" if (stooq_status == "✅" and price > 0) else "PARTIAL"
        # Sofort-Log für die Konsole
        ts = datetime.now().strftime('%H:%M:%S')
        icon = "✅" if status == "FULL" else "⚠️"
        print(f"[{ts}] {icon} {ticker:8} | Preis: {price:10.2f} | Stooq: {stooq_status}")
        
        return {"ticker": ticker, "price": price, "hist": hist_df, "live": live_5m, "gap": gap_1d, "status": status}

    def safe_store(self, res):
        if not res or res['price'] == 0: return
        with storage_lock:
            ticker = res['ticker']
            
            # A. Live Ticker
            if res['live'] is not None and not res['live'].empty:
                df_l = res['live'].copy()
                df_l['Ticker'] = ticker
                self._atomic_save(df_l.reset_index(), LIVE_TICKER_FEATHER, "feather")

            # B. Heritage (Multi-Level: Jahrzehnt/Jahr)
            if res['hist'] is not None and not res['hist'].empty:
                combined = pd.concat([res['hist'], res['gap'] or pd.DataFrame()]).sort_index()
                combined = combined[~combined.index.duplicated(keep='last')]
                combined['Year'] = combined.index.year
                combined['Decade'] = (combined['Year'] // 10) * 10
                
                for (dec, yr), group in combined.groupby(['Decade', 'Year']):
                    path = f"{HERITAGE_ROOT}{int(dec)}s/heritage_{int(yr)}.parquet"
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    g = group.copy(); g['Ticker'] = ticker
                    self._atomic_save(g, path, "parquet")

            # Audit & Sync-Zeitstempel
            self.audit_logs.append(f"{ticker}: {res['price']} | Stooq: {res['status']}")
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
        print(f"=== AUREUM SENTINEL V205 | 10k SYNC & LOGGING START [{datetime.now()}] ===")
        self.expand_pool_via_wiki()
        
        # Sortiere: Am längsten nicht synchronisierte zuerst
        self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
        
        # Batch für diesen Lauf (alle verfügbaren Ticker)
        batch = self.pool[:12000]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.fetch_task, a) for a in batch]
            for f in concurrent.futures.as_completed(futures):
                self.safe_store(f.result())

        with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write(f"=== HERITAGE AUDIT REPORT {datetime.now()} ===\n")
            f.write("\n".join(self.audit_logs))
        print(f"=== Zyklus beendet. Pool: {len(self.pool)} Assets ===")

if __name__ == "__main__":
    AureumSentinel().run()
