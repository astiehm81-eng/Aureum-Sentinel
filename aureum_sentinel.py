import pandas as pd
import yfinance as yf
import os
import json
import time
import threading
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION V110 ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
ANCHOR_FILE = "anchors_memory.json"

ANCHOR_THRESHOLD = 0.0005  
REFRESH_RATE = 300         
RUNTIME_LIMIT = 800        

db_lock = threading.Lock()
anchors = {}

def force_log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    sys.stdout.write(f"[{ts}] {msg}\n")
    sys.stdout.flush()

def heritage_healer():
    """Engine B: Verheiratung von Yahoo-Historie mit dem Vault."""
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, "r") as f: pool = json.load(f)

    force_log(f"🩹 Engine B: Prüfe Heritage für {len(pool)} Assets...")

    def heal(asset):
        symbol = asset['symbol']
        path = os.path.join(HERITAGE_DIR, f"{symbol}_history.parquet")
        
        # Heile wenn Datei fehlt oder älter als 24h
        if not os.path.exists(path) or (time.time() - os.path.getmtime(path)) > 86400:
            try:
                t = yf.Ticker(symbol)
                df_hist = t.history(period="max")
                if not df_hist.empty:
                    # Alles vor der letzten Woche wegschreiben
                    cutoff = datetime.now() - timedelta(days=7)
                    df_hist = df_hist[df_hist.index < cutoff]
                    df_hist['Ticker'] = symbol
                    df_hist.to_parquet(path)
                    return f"🩹 HERITAGE REPAIRED: {symbol}"
            except: pass
        return None

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(heal, a) for a in pool]
        for future in as_completed(futures):
            res = future.result()
            if res: force_log(res)

def ticker_engine():
    start_time = time.time()
    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: pool = json.load(f)
        else: pool = []

        force_log(f"💓 HEARTBEAT - Puls-Check für {len(pool)} Assets.")
        
        # ... (Rest der Engine A Logik wie bisher) ...
        # Hier habe ich zur Kürzung nur den Kern gelassen
        
        time.sleep(REFRESH_RATE)

if __name__ == "__main__":
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    if os.path.exists(ANCHOR_FILE):
        with open(ANCHOR_FILE, "r") as f: anchors = json.load(f)
    
    heritage_healer()
    # Hier folgt der Aufruf für Engine A...
