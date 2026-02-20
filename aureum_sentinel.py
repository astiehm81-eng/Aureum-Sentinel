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
REPORT_FILE = "coverage_report.txt"

ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      
TOTAL_RUNTIME = 1100

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        self._sanitize_environment() # Bereinigung vor Start
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        self._sync_anchors()

    def _sanitize_environment(self):
        """Löscht alles Nicht-Benötigte und migriert alte Strukturen."""
        log("CLEANUP", "Initialisiere System-Bereinigung...")
        
        # 1. Migration von heritage_vault zu heritage
        old_dir = "heritage_vault"
        if os.path.exists(old_dir):
            log("CLEANUP", f"Migriere Daten von {old_dir} nach {HERITAGE_DIR}...")
            if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
            for item in os.listdir(old_dir):
                s = os.path.join(old_dir, item)
                d = os.path.join(HERITAGE_DIR, item)
                if os.path.isfile(s): shutil.move(s, d)
            shutil.rmtree(old_dir)

        # 2. Temporäre Dateien löschen
        for root, dirs, files in os.walk("."):
            for file in files:
                if file.endswith(".tmp") or file.endswith(".temp"):
                    os.remove(os.path.join(root, file))

        # 3. Bekannte Altlasten-Dateien (aus früheren Versionen)
        legacy_files = ["live_buffer.parquet", "system.lock", "data_gaps.json"]
        for f in legacy_files:
            if os.path.exists(f): os.remove(f)
            
        log("CLEANUP", "System ist nun im Soll-Zustand (Heritage-Standard).")

    def _sync_anchors(self):
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass
        
        if not self.anchors:
            recent_file = os.path.join(HERITAGE_DIR, "heritage_2020s.parquet")
            if os.path.exists(recent_file):
                try:
                    df = pd.read_parquet(recent_file)
                    self.anchors = df.sort_values('Date').groupby('Ticker').last()['Price'].to_dict()
                    log("INIT", f"⚓ {len(self.anchors)} Anker synchronisiert.")
                except: pass

    def atomic_write(self, df, path, format="parquet"):
        tmp = path + ".tmp"
        try:
            df = df.dropna(subset=['Price'])
            df = df[df['Price'] > 0]
            if format == "parquet":
                if os.path.exists(path):
                    old = pd.read_parquet(path)
                    df = pd.concat([old, df]).drop_duplicates(subset=['Date', 'Ticker']).sort_values('Date')
                df.to_parquet(tmp, compression='zstd', index=False)
            else:
                df.to_feather(tmp)
            os.replace(tmp, path)
        except Exception as e:
            if os.path.exists(tmp): os.remove(tmp)
            log("ERROR", f"Schreibfehler: {e}")

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        log("STATUS", f"Puls-Check: {len(pool)} Assets aktiv.")
        ticker_batch, anchor_batch = [], []
        now = datetime.now().replace(microsecond=0)

        with ThreadPoolExecutor(max_workers=60) as exe:
            futures = {exe.submit(lambda s: (s, yf.Ticker(s).fast_info.get('last_price')), a['symbol']): a['symbol'] for a in pool}
            for f in as_completed(futures):
                sym, price = f.result()
                if price and price > 0:
                    log("TICK", f"💓 {sym}: {price}")
                    ticker_batch.append({"Date": now, "Ticker": sym, "Price": price})
                    
                    last = self.anchors.get(sym)
                    if last is None or abs(price - last) / last >= ANCHOR_THRESHOLD:
                        self.anchors[sym] = price
                        anchor_batch.append({"Date": now, "Ticker": sym, "Price": price})

        if ticker_batch:
            self.atomic_write(pd.DataFrame(ticker_batch), TICKER_FILE, "feather")
            if anchor_batch:
                df_anchor = pd.DataFrame(anchor_batch)
                df_anchor['Decade'] = (pd.to_datetime(df_anchor['Date']).dt.year // 10) * 10
                for decade, chunk in df_anchor.groupby('Decade'):
                    path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
                    self.atomic_write(chunk.drop(columns=['Decade']), path)
            
            with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            log("PROGRESS", f"✅ {len(ticker_batch)} Ticks / {len(anchor_batch)} Anker.")

if __name__ == "__main__":
    def finder_task(k):
        client = genai.Client(api_key=k)
        while True:
            try:
                log("FINDER", "🔎 Suche neue ISINs...")
                r = client.models.generate_content(model="gemini-2.0-flash", contents="Nenne 50 Nasdaq/NYSE Tickersymbole. NUR JSON: ['AAPL', ...]")
                new_list = json.loads(r.text.strip().replace("```json", "").replace("```", ""))
                with open(POOL_FILE, "r") as f: pool = json.load(f)
                existing = {a['symbol'] for a in pool}
                added = [s.upper() for s in new_list if s.upper() not in existing]
                if added:
                    for s in added: pool.append({"symbol": s, "added_at": datetime.now().isoformat()})
                    with open(POOL_FILE, "w") as f: json.dump(pool, f, indent=4)
                    log("FINDER", f"✨ Pool +{len(added)} Assets. Stichprobe: {random.choice(added)}")
                time.sleep(600)
            except: time.sleep(60)

    key = os.getenv("GEMINI_API_KEY")
    p = multiprocessing.Process(target=finder_task, args=(key,))
    p.start()
    
    try:
        sentinel = AureumSentinel()
        while True:
            sentinel.run_cycle()
            time.sleep(PULSE_INTERVAL)
    finally:
        p.terminate()
