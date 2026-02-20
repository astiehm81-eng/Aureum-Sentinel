import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
import multiprocessing
from datetime import datetime
from google import genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION (EISERNER STANDARD) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
REPORT_FILE = "coverage_report.txt"
ANCHOR_FILE = "anchors_memory.json"

ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      
TOTAL_RUNTIME = 1100

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

# --- ROBUSTE SPEICHER-LOGIK ---
def safe_atomic_save(df, target_path, format="parquet"):
    temp_path = target_path + ".tmp"
    try:
        if format == "parquet":
            df.to_parquet(temp_path, index=False)
        else:
            df.to_feather(temp_path)
        os.replace(temp_path, target_path)
    except:
        if os.path.exists(temp_path): os.remove(temp_path)

def save_to_decade(df):
    if df.empty: return
    df = df.copy()
    df['Year'] = pd.to_datetime(df['Date']).dt.year
    for year in df['Year'].unique():
        decade = (year // 10) * 10
        path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
        decade_df = df[df['Year'].between(decade, decade + 9)].drop(columns=['Year'])
        if os.path.exists(path):
            try:
                old_df = pd.read_parquet(path)
                decade_df = pd.concat([old_df, decade_df]).drop_duplicates(subset=['Date', 'Ticker'])
            except: pass
        safe_atomic_save(decade_df, path)

class AureumSentinel:
    def __init__(self):
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass
        self.is_cold_start = not bool(self.anchors)

    def run_pulse(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        log("STATUS", f"Puls-Check: {len(pool)} ISINs registriert. " + 
            ("[KALTSTART]" if self.is_cold_start else "[ÜBERWACHUNG]"))
        
        ticker_updates = []
        anchor_updates = []
        now = datetime.now().replace(microsecond=0)

        with ThreadPoolExecutor(max_workers=60) as exe:
            futures = [exe.submit(lambda s: (s, yf.Ticker(s).fast_info.get('last_price')), a['symbol']) for a in pool]
            for f in as_completed(futures):
                sym, price = f.result()
                if price:
                    # Ticker bekommt immer einen Eintrag
                    ticker_updates.append({"Date": now, "Ticker": sym, "Price": price})
                    
                    # Anker-Logik
                    last_price = self.anchors.get(sym)
                    # Bedingung: Entweder Kaltstart ODER 0,05% Differenz
                    if self.is_cold_start or last_price is None or abs(price - last_price) / last_price >= ANCHOR_THRESHOLD:
                        self.anchors[sym] = price
                        anchor_updates.append({"Date": now, "Ticker": sym, "Price": price})

        if ticker_updates:
            # 1. Ticker Stream (Feather)
            safe_atomic_save(pd.DataFrame(ticker_updates), TICKER_FILE, format="feather")
            
            # 2. Heritage Decade Vault (Parquet)
            if anchor_updates:
                save_to_decade(pd.DataFrame(anchor_updates))
            
            # Anker-Memory sichern
            with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            
            log("PROGRESS", f"✅ {len(ticker_updates)} Assets erfasst. {len(anchor_updates)} Anker gesetzt.")
            self.is_cold_start = False # Nach dem ersten erfolgreichen Puls beendet

            # Report schreiben
            with open(REPORT_FILE, "w") as rf:
                rf.write(f"Aureum Sentinel Status - {now}\n")
                rf.write(f"Gesamt Pool: {len(pool)}\n")
                rf.write(f"Aktive Ticker: {len(ticker_updates)}\n")
                rf.write(f"Initial-Anker gesetzt: {self.is_cold_start}\n")

if __name__ == "__main__":
    key = os.getenv("GEMINI_API_KEY")
    
    # Expansion-Prozess
    def expansion_task(k):
        client = genai.Client(api_key=k)
        while True:
            try:
                log("FINDER", "Suche neue ISINs...")
                prompt = "Nenne 50 neue Nasdaq/DAX Tickersymbole. NUR JSON-Liste: ['AAPL', ...]"
                resp = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
                new_list = json.loads(resp.text.strip().replace("```json", "").replace("```", ""))
                with open(POOL_FILE, "r") as f: pool = json.load(f)
                existing = {a['symbol'] for a in pool}
                added = 0
                for s in new_list:
                    if s.upper() not in existing:
                        pool.append({"symbol": s.upper(), "added_at": datetime.now().isoformat()})
                        added += 1
                if added > 0:
                    with open(POOL_FILE, "w") as f: json.dump(pool, f, indent=4)
                    log("FINDER", f"✨ Pool erweitert: +{added} Assets.")
                time.sleep(300)
            except: time.sleep(60)

    p_exp = multiprocessing.Process(target=expansion_task, args=(key,))
    p_exp.start()
    
    try:
        sentinel = AureumSentinel()
        for _ in range(4): # 4 Pulse pro Workflow (ca. 20 Min)
            sentinel.run_pulse()
            time.sleep(PULSE_INTERVAL)
    finally:
        p_exp.terminate()
        log("SYSTEM", "Zyklus beendet.")
