import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
import threading
from datetime import datetime

# --- HOCHLEISTUNGS-KONFIGURATION ---
HERITAGE_ROOT = "heritage/"
LIVE_TICKER_FEATHER = "heritage/live_ticker.feather"
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
MAX_WORKERS = 20  # Worker hochgefahren
storage_lock = threading.Lock() # Globaler Schutz gegen Race-Conditions

class AureumSentinel:
    def __init__(self):
        os.makedirs(HERITAGE_ROOT, exist_ok=True)
        self.load_pool()
        self.audit_logs = []

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: 
                self.pool = json.load(f)
            # Priorisierung: Assets, die am längsten kein Update hatten
            self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
        else: 
            self.pool = []

    def log_market(self, ticker, price, status):
        ts = datetime.now().strftime('%H:%M:%S')
        icon = "✅" if status == "FULL" else "⚠️"
        print(f"[{ts}] {icon} {ticker:8} | Preis: {price:10.2f} | Status: {status}")

    def fetch_worker_task(self, asset):
        """Worker-Logik: Daten sammeln (Kein Schreiben)"""
        ticker = asset['symbol']
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
        
        live_5m, gap_fill_1d, hist_df = None, None, None
        current_price = 0.0
        stooq_status = "❌"

        try:
            # 1. Yahoo (Live & Gap)
            stock = yf.Ticker(ticker)
            live_5m = stock.history(period="7d", interval="5m")
            gap_fill_1d = stock.history(period="1mo", interval="1d")
            if not live_5m.empty:
                current_price = live_5m['Close'].iloc[-1]

            # 2. Stooq (Heimatmarkt)
            r = requests.get(f"https://stooq.com/q/d/l/?s={ticker.lower()}&i=d", headers=headers, timeout=5)
            if len(r.content) > 200:
                hist_df = pd.read_csv(io.StringIO(r.text), index_col='Date', parse_dates=True)
                stooq_status = "✅"
        except Exception as e:
            pass

        status = "FULL" if (stooq_status == "✅" and current_price > 0) else "PARTIAL"
        self.log_market(ticker, current_price, status)
        
        return {
            "ticker": ticker, "price": current_price, "hist": hist_df, 
            "live": live_5m, "gap": gap_fill_1d, "stooq": stooq_status
        }

    def safe_storage_manager(self, res):
        """Zentraler Manager: Nur ein Thread schreibt zur Zeit."""
        if not res: return
        
        with storage_lock:
            ticker = res['ticker']
            
            # A. Live-Ticker (Feather)
            if res['live'] is not None and not res['live'].empty:
                df_live = res['live'].copy()
                df_live['Ticker'] = ticker
                self._atomic_save(df_live.reset_index(), LIVE_TICKER_FEATHER, "feather")

            # B. Heritage Deep-Storage (Jahrzehnt-Ordner / Jahres-Parquet)
            if res['hist'] is not None:
                # Heirat Stooq + Yahoo Gap
                combined = res['hist']
                if res['gap'] is not None:
                    combined = pd.concat([res['hist'], res['gap']]).sort_index()
                    combined = combined[~combined.index.duplicated(keep='last')]
                
                # Partitionierung
                combined['Year'] = combined.index.year
                combined['Decade'] = (combined['Year'] // 10) * 10
                
                for (decade, year), group in combined.groupby(['Decade', 'Year']):
                    decade_dir = f"{HERITAGE_ROOT}{int(decade)}s/"
                    os.makedirs(decade_dir, exist_ok=True)
                    path = f"{decade_dir}heritage_{int(year)}.parquet"
                    
                    group = group.copy()
                    group['Ticker'] = ticker
                    self._atomic_save(group, path, "parquet")

            # Update Sync-Timestamp im Pool
            self.audit_logs.append(f"{ticker}: {res['price']} | Stooq: {res['stooq']}")
            for a in self.pool:
                if a['symbol'] == ticker:
                    a['last_sync'] = datetime.now().isoformat()
                    break

    def _atomic_save(self, df, path, fmt):
        """Sichert Dateiintegrität durch .tmp und .replace"""
        if os.path.exists(path):
            try:
                old = pd.read_parquet(path) if fmt == "parquet" else pd.read_feather(path)
                combined = pd.concat([old, df])
                # Deduplizierung über Ticker und Zeitstempel
                idx = combined.index.name or ('Date' if 'Date' in combined.columns else 'Datetime')
                combined = combined.reset_index().drop_duplicates(subset=[idx, 'Ticker']).set_index(idx)
            except: combined = df
        else: combined = df
        
        tmp = path + ".tmp"
        if fmt == "parquet": combined.to_parquet(tmp)
        else: combined.to_feather(tmp)
        os.replace(tmp, path)

    def run(self):
        print(f"=== AUREUM SENTINEL V202 | WORKER BOOST (20) START [{datetime.now()}] ===")
        # Batch von 200 Assets für hohe Auslastung
        batch = self.pool[:200]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.fetch_worker_task, a) for a in batch]
            for f in concurrent.futures.as_completed(futures):
                self.safe_storage_manager(f.result())

        # Finales Pool-Update & Audit
        with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write(f"=== HERITAGE AUDIT {datetime.now()} ===\n")
            f.write("\n".join(self.audit_logs))
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Batch abgeschlossen.")

if __name__ == "__main__":
    AureumSentinel().run()
