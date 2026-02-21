import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
import threading
import time
from datetime import datetime

# --- KONFIGURATION EISERNER STANDARD ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage/sentinel_audit.log"
MAX_WORKERS = 60
ANCHOR_THRESHOLD = 0.0005  # 0.05% Eiserner Standard
SKOOQ_URL = "https://stooq.com/q/d/l/?s={ticker}&i=d"

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.pool_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.stats = {
            "received": 0, "anchors": 0, "new_isins": 0, 
            "merges": 0, "errors": 0, "total": 0, "start": time.time()
        }

    def log(self, level, ticker, message):
        """Zentrales Logging für Marktabdeckung und Status"""
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] [{level:7}] {ticker:10} | {message}"
        print(line, flush=True)
        with self.log_lock:
            with open(AUDIT_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def apply_iron_anchor(self, df, ticker):
        """0,05% Anker-Regel & Logging der Reduktion"""
        if df.empty: return df
        df = df.sort_values('Date').copy()
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        
        initial_count = len(df)
        anchors = [df.iloc[0].to_dict()]
        for i in range(1, len(df)):
            last_p = anchors[-1]['Close']
            curr_p = df.iloc[i]['Close']
            if abs((curr_p / last_p) - 1) >= ANCHOR_THRESHOLD:
                anchors.append(df.iloc[i].to_dict())
        
        res = pd.DataFrame(anchors)
        with self.stats_lock: 
            self.stats["anchors"] += len(res)
        self.log("ANCHOR", ticker, f"Anker gesetzt: {len(res)} (von {initial_count} Rohdaten)")
        return res

    def save_heritage(self, df, ticker):
        """Mergen & Speichern mit Integritäts-Log"""
        try:
            for year, group in df.groupby(df['Date'].dt.year):
                path = f"{HERITAGE_ROOT}{(int(year)//10)*10}s/heritage_{int(year)}.parquet"
                os.makedirs(os.path.dirname(path), exist_ok=True)
                
                status = "NEW"
                if os.path.exists(path):
                    old = pd.read_parquet(path)
                    group = pd.concat([old, group]).drop_duplicates(subset=['Date'])
                    status = "MERGE"
                
                group.to_parquet(path, index=False, compression='snappy')
                with self.stats_lock: self.stats["merges"] += 1
                self.log(status, ticker, f"Daten für Jahr {int(year)} gesichert ({len(group)} Zeilen)")
        except Exception as e:
            self.log("ERROR", ticker, f"Speicherfehler: {str(e)}")
            with self.stats_lock: self.stats["errors"] += 1

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = [{"symbol": "SAP.DE"}]

    def discover(self, ticker):
        """Discovery-Logik für 99% Abdeckung"""
        base = ticker.split('.')[0]
        for s in ['.DE', '.F', '.AS', '.L', '']:
            cand = f"{base}{s}"
            with inspector.pool_lock:
                if not any(a['symbol'] == cand for a in self.pool):
                    self.pool.append({"symbol": cand, "last_sync": "1900-01-01"})
                    inspector.stats["new_isins"] += 1
                    inspector.log("DISCOV", cand, f"Neue ISIN durch {ticker} gefunden")

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # Skooq & Yahoo Heirat
            r = requests.get(SKOOQ_URL.format(ticker=ticker), timeout=10)
            hist = pd.read_csv(io.StringIO(r.text), parse_dates=['Date']) if len(r.content) > 300 else pd.DataFrame()
            
            stock = yf.Ticker(ticker)
            recent = stock.history(period="7d", interval="5m").reset_index()
            
            if recent.empty and hist.empty:
                inspector.log("EMPTY", ticker, "Keine Daten bei Skooq oder Yahoo gefunden")
                return "EMPTY"

            # Merge-Logik
            if not recent.empty:
                recent.rename(columns={'Datetime': 'Date'}, inplace=True, errors='ignore')
                cutoff = recent['Date'].min().tz_localize(None)
                hist = hist[hist['Date'] < cutoff] if not hist.empty else hist
                combined = pd.concat([hist, recent], ignore_index=True)
                inspector.log("SYNC", ticker, f"Heirat vollzogen: {len(hist)} (Skooq) + {len(recent)} (Yahoo)")
            else:
                combined = hist
                inspector.log("SYNC", ticker, "Nur Skooq-Historie verfügbar")

            clean_df = inspector.apply_iron_anchor(combined, ticker)
            inspector.save_heritage(clean_df, ticker)
            
            if len(self.pool) < 12000: self.discover(ticker)
            return "OK"
        except Exception as e:
            inspector.log("FAIL", ticker, f"Worker-Fehler: {str(e)}")
            return "ERR"

    def print_dashboard(self):
        s = inspector.stats
        elapsed = (time.time() - s["start"]) / 60
        cov = (s["total"] / len(self.pool)) * 100 if self.pool else 0
        
        print(f"\n" + "="*75)
        print(f" AUREUM SENTINEL V235 | POOL-SIZE: {len(self.pool):5} | NEU: {s['new_isins']}")
        print(f" ABDECKUNG: {cov:6.2f}% | MERGES: {s['merges']:5} | FEHLER: {s['errors']}")
        print(f" SPEED: {s['total']/elapsed:6.1f} Ast/Min | ANKER: {s['anchors']}")
        print("="*75 + "\n")

    def run(self):
        inspector.log("SYSTEM", "START", f"Zyklus gestartet mit {len(self.pool)} ISINs")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in range(0, len(self.pool), 400):
                batch = self.pool[i:i+400]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                for f in concurrent.futures.as_completed(futures):
                    inspector.stats["total"] += 1
                    if inspector.stats["total"] % 40 == 0: self.print_dashboard()
                
                with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)
        inspector.log("SYSTEM", "END", "Zyklus beendet")

if __name__ == "__main__":
    AureumSentinel().run()
