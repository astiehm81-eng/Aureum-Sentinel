import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
import multiprocessing
from datetime import datetime, timedelta
from google import genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION (EISERNER STANDARD) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
BUFFER_FILE = os.path.join(HERITAGE_DIR, "live_buffer.parquet")
REPORT_FILE = "coverage_report.txt" # Der gewünschte Status-Report
LOCK_FILE = "system.lock"

ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      
TOTAL_RUNTIME = 1100

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

# --- ENGINE: DATA FETCH & VERIFY ---
def fetch_and_verify(symbol):
    try:
        t = yf.Ticker(symbol)
        price = t.fast_info.get('last_price')
        hist = t.history(period="1d", interval="5m").reset_index()
        hist = hist.rename(columns={'Datetime': 'Date', 'Close': 'Price'})[['Date', 'Price']]
        hist = hist[hist['Price'] > 0]
        return symbol, price, hist, True
    except:
        return symbol, None, pd.DataFrame(), False

class AureumSentinel:
    def __init__(self):
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.stats = {"total_isins": 0, "active_hits": 0, "anchors_set": 0, "healed_gaps": 0}

    def write_report(self):
        """Erstellt die gewünschte txt-Datei mit dem aktuellen Stand."""
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        content = f"""AUREUM SENTINEL - STATUS REPORT
====================================
Zeitstempel:     {ts}
ISIN Pool Größe: {self.stats['total_isins']}
Aktive Ticker:   {self.stats['active_hits']}
Anker im Vault:  {self.stats['anchors_set']}
Geheilte Lücken: {self.stats['healed_gaps']}
System-Status:   Betriebsbereit (V125)
===================================="""
        with open(REPORT_FILE, "w") as f:
            f.write(content)

    def safe_save(self, ticker_df, heritage_df):
        while os.path.exists(LOCK_FILE): time.sleep(0.05)
        try:
            with open(LOCK_FILE, "w") as f: f.write("1")
            # Ticker (Feather)
            if not ticker_df.empty:
                if os.path.exists(TICKER_FILE):
                    old = pd.read_feather(TICKER_FILE)
                    ticker_df = pd.concat([old, ticker_df]).tail(20000)
                ticker_df.to_feather(TICKER_FILE)
            
            # Vault (Parquet)
            if not heritage_df.empty:
                if os.path.exists(BUFFER_FILE):
                    old_v = pd.read_parquet(BUFFER_FILE)
                    heritage_df = pd.concat([old_v, heritage_df]).drop_duplicates(subset=['Date', 'Ticker'])
                heritage_df.to_parquet(BUFFER_FILE, index=False)
                self.stats['anchors_set'] = len(heritage_df)
        finally:
            if os.path.exists(LOCK_FILE): os.remove(LOCK_FILE)

    def run_pulse(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        self.stats['total_isins'] = len(pool)
        
        log("SENTINEL", f"Puls-Check: {len(pool)} Assets.")
        ticker_list, heritage_list = [], []
        hits = 0
        now = datetime.now().replace(microsecond=0)

        with ThreadPoolExecutor(max_workers=60) as exe:
            futures = [exe.submit(fetch_and_verify, a['symbol']) for a in pool]
            for f in as_completed(futures):
                sym, price, v_hist, success = f.result()
                if success:
                    hits += 1
                    if not v_hist.empty:
                        v_hist['Ticker'] = sym
                        heritage_list.append(v_hist)
                    if price:
                        ticker_list.append({"Date": now, "Ticker": sym, "Price": price})

        self.stats['active_hits'] = hits
        if ticker_list or heritage_list:
            self.safe_save(pd.DataFrame(ticker_list), pd.concat(heritage_list) if heritage_list else pd.DataFrame())
            self.write_report()
            log("SYSTEM", f"Report aktualisiert. Ticker: {hits}/{len(pool)}")

    def loop(self):
        start = time.time()
        while (time.time() - start) < TOTAL_RUNTIME:
            c_start = time.time()
            self.run_pulse()
            wait = max(10, PULSE_INTERVAL - (time.time() - c_start))
            time.sleep(wait)

if __name__ == "__main__":
    key = os.getenv("GEMINI_API_KEY")
    # Expansion-Prozess
    p_gemini = None
    if key and len(key) > 10:
        def gemini_expansion(k):
            client = genai.Client(api_key=k)
            while True:
                try: 
                    # Expansion-Logik
                    time.sleep(600)
                except: time.sleep(60)
        p_gemini = multiprocessing.Process(target=gemini_expansion, args=(key,))
        p_gemini.start()

    try:
        AureumSentinel().loop()
    finally:
        if p_gemini: p_gemini.terminate()
        log("SYSTEM", "Zyklus beendet.")
