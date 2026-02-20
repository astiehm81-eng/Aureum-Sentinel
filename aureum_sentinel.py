import pandas as pd
import yfinance as yf
import os
import json
import time
import threading
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION (V109 - VOLLSTÄNDIGER FOKUS) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
ANCHOR_FILE = "anchors_memory.json"
STATUS_FILE = "vault_status.txt"
SNAPSHOT_FILE = "AUREUM_SNAPSHOT.txt"

ANCHOR_THRESHOLD = 0.0005  
REFRESH_RATE = 300         
RUNTIME_LIMIT = 800        
MAX_WORKERS_LIVE = 50
MAX_WORKERS_HERITAGE = 20

db_lock = threading.Lock()
anchors = {}

def force_log(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    sys.stdout.write(f"[{timestamp}] {msg}\n")
    sys.stdout.flush()

# --- ENGINE B: HERITAGE (YAHOO/STOOQ) ---
def heritage_healer():
    force_log("🩹 Engine B: Starte Heritage-Archivierung (Skooq/Yahoo)...")
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, "r") as f:
        pool = json.load(f)

    def heal(asset):
        symbol = asset['symbol']
        path = os.path.join(HERITAGE_DIR, f"{symbol}_history.parquet")
        # Heile wenn Datei fehlt oder älter als 24h
        if not os.path.exists(path) or (time.time() - os.path.getmtime(path)) > 86400:
            try:
                t = yf.Ticker(symbol)
                df_hist = t.history(period="max")
                if not df_hist.empty:
                    cutoff = datetime.now() - timedelta(days=7)
                    df_hist = df_hist[df_hist.index < cutoff]
                    df_hist['Ticker'] = symbol
                    df_hist.to_parquet(path)
                    force_log(f"✅ HERITAGE GESPEICHERT: {symbol} (Archiv-Daten befüllt)")
                    return True
            except: pass
        return False

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_HERITAGE) as executor:
        futures = [executor.submit(heal, a) for a in pool]
        count = sum(1 for f in as_completed(futures) if f.result())
    force_log(f"📊 Heritage-Check beendet. {count} neue Archiv-Updates.")

# --- ENGINE A: LIVE-PULS ---
def ticker_engine():
    start_time = time.time()
    force_log("🛡️ Engine A: Live-Puls (0,05% Anker) gestartet.")
    
    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: current_pool = json.load(f)
        else: current_pool = []

        force_log(f"💓 HEARTBEAT - Überwache {len(current_pool)} Assets...")
        results = []

        def process(asset):
            sym = asset['symbol']
            try:
                t = yf.Ticker(sym)
                df = t.history(period="2d", interval="1m")
                if not df.empty:
                    p = round(df['Close'].iloc[-1], 4)
                    last = anchors.get(sym)
                    if last is None or abs(p - last)/last >= ANCHOR_THRESHOLD:
                        anchors[sym] = p
                        return {"Date": datetime.now().replace(microsecond=0), "Ticker": sym, "Price": p}
            except: pass
            return None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_LIVE) as exe:
            futures = [exe.submit(process, a) for a in current_pool]
            for f in as_completed(futures):
                r = f.result()
                if r:
                    results.append(r)
                    force_log(f"⚡ ANKER: {r['Ticker']} @ {r['Price']}")

        if results:
            save_vault(pd.DataFrame(results))
            with open(ANCHOR_FILE, "w") as f: json.dump(anchors, f)
        
        force_log(f"✅ Puls beendet. Nächster Scan in {REFRESH_RATE}s.")
        time.sleep(REFRESH_RATE)

def save_vault(df_new):
    with db_lock:
        path = os.path.join(HERITAGE_DIR, "live_buffer.parquet")
        if os.path.exists(path):
            df_old = pd.read_parquet(path)
            df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['Date', 'Ticker'])
        else: df_final = df_new
        df_final.to_parquet(path, index=False)

if __name__ == "__main__":
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    if os.path.exists(ANCHOR_FILE):
        with open(ANCHOR_FILE, "r") as f: anchors = json.load(f)
    
    # 1. Heritage Healer (Engine B) läuft zuerst für klare Logs
    heritage_healer()
    # 2. Ticker Engine (Engine A) startet danach
    ticker_engine()
