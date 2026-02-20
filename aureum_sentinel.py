import pandas as pd
import yfinance as yf
import os
import json
import time
import multiprocessing
import shutil
import random
from datetime import datetime
from google import genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION (EISERNER STANDARD) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
ANCHOR_FILE = "anchors_memory.json"

ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        self._sanitize_environment()
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        self.is_initial_start = True
        self.known_assets = set()
        self._sync_and_audit()

    def _sanitize_environment(self):
        log("CLEANUP", "System-Bereinigung läuft...")
        old_dir = "heritage_vault"
        if os.path.exists(old_dir):
            if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
            for item in os.listdir(old_dir):
                shutil.move(os.path.join(old_dir, item), os.path.join(HERITAGE_DIR, item))
            shutil.rmtree(old_dir)
        for f in ["live_buffer.parquet", "system.lock", "current_buffer.parquet"]:
            if os.path.exists(f): os.remove(f)

    def _sync_and_audit(self):
        # Scannt den Bestand, um zu wissen, wer schon eine Historie hat
        for f in os.listdir(HERITAGE_DIR):
            if f.endswith(".parquet") and f.startswith("heritage_"):
                try:
                    df = pd.read_parquet(os.path.join(HERITAGE_DIR, f))
                    self.known_assets.update(df['Ticker'].unique())
                except: pass
        
        if os.path.exists(ANCHOR_FILE):
            with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
        
        log("HERITAGE", f"📊 Datenbasis: {len(self.known_assets)} Assets mit Historie vorhanden.")

    def fetch_deep_history(self, symbol):
        """Holt die maximale Historie für neue Assets."""
        try:
            t = yf.Ticker(symbol)
            # 'max' holt alles verfügbare (Tage/Jahre)
            hist = t.history(period="max", interval="1d").reset_index()
            if hist.empty: return symbol, None, pd.DataFrame()
            
            hist = hist.rename(columns={'Date': 'Date', 'Datetime': 'Date', 'Close': 'Price'})
            hist['Date'] = pd.to_datetime(hist['Date'], utc=True).dt.tz_localize(None)
            return symbol, t.fast_info.get('last_price'), hist[['Date', 'Price']]
        except:
            return symbol, None, pd.DataFrame()

    def atomic_write(self, df, path):
        tmp = path + ".tmp"
        try:
            df = df.dropna(subset=['Price'])
            df = df[df['Price'] > 0]
            if os.path.exists(path):
                old = pd.read_parquet(path)
                df = pd.concat([old, df]).drop_duplicates(subset=['Date', 'Ticker']).sort_values('Date')
            df.to_parquet(tmp, compression='zstd', index=False)
            os.replace(tmp, path)
        except Exception as e:
            if os.path.exists(tmp): os.remove(tmp)
            log("ERROR", f"Schreibfehler: {e}")

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        log("STATUS", f"Puls-Check: {len(pool)} Assets.")
        ticker_batch, heritage_updates = [], []
        now = datetime.now().replace(microsecond=0)

        with ThreadPoolExecutor(max_workers=40) as exe:
            # Wenn Asset unbekannt -> Deep History, sonst nur aktueller Preis
            tasks = []
            for a in pool:
                sym = a['symbol']
                if sym not in self.known_assets or self.is_initial_start:
                    tasks.append(exe.submit(self.fetch_deep_history, sym))
                else:
                    tasks.append(exe.submit(lambda s: (s, yf.Ticker(s).fast_info.get('last_price'), pd.DataFrame()), sym))

            for f in as_completed(tasks):
                sym, price, hist = f.result()
                if price:
                    log("TICK", f"💓 {sym}: {price}")
                    ticker_batch.append({"Date": now, "Ticker": sym, "Price": price})
                    
                    # Verarbeite Historie (falls vorhanden/neu)
                    if not hist.empty:
                        heritage_updates.append(hist.assign(Ticker=sym))
                        self.known_assets.add(sym)
                    
                    # Anker-Logik für Live-Daten
                    last = self.anchors.get(sym)
                    if last is None or abs(price - last) / last >= ANCHOR_THRESHOLD:
                        self.anchors[sym] = price
                        heritage_updates.append(pd.DataFrame([{"Date": now, "Ticker": sym, "Price": price}]))

        if ticker_batch:
            pd.DataFrame(ticker_batch).to_feather(TICKER_FILE)
            if heritage_updates:
                df_all = pd.concat(heritage_updates)
                df_all['Decade'] = (pd.to_datetime(df_all['Date']).dt.year // 10) * 10
                for decade, chunk in df_all.groupby('Decade'):
                    path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
                    self.atomic_write(chunk.drop(columns=['Decade']), path)
            
            with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            coverage = (len(self.known_assets) / len(pool)) * 100
            log("PROGRESS", f"✅ Zyklus beendet. Abdeckung: {len(self.known_assets)}/{len(pool)} ({coverage:.1f}%)")
            self.is_initial_start = False

if __name__ == "__main__":
    # Finder Loop bleibt gleich...
    key = os.getenv("GEMINI_API_KEY")
    # ... (multiprocessing start etc wie in V134)
