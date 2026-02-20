import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION (PRÄZISIONS-STEUERUNG) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
ANCHOR_FILE = "anchors_memory.json"

ANCHOR_THRESHOLD = 0.0005  
PULSE_INTERVAL = 300       # 5 Minuten
TOTAL_RUNTIME = 900        # 15 Minuten Gesamtlaufzeit
MAX_WORKERS = 50

anchors = {}

def log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    sys.stdout.write(f"[{ts}] {msg}\n")
    sys.stdout.flush()

def process_pulse(pool):
    log(f"💓 Puls-Check für {len(pool)} Assets...")
    results = []
    
    def fetch(asset):
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

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = [exe.submit(fetch, a) for a in pool]
        for f in as_completed(futures):
            res = f.result()
            if res: results.append(res)
    return results

def save_data(df_new):
    if df_new.empty: return
    path = os.path.join(HERITAGE_DIR, "live_buffer.parquet")
    if os.path.exists(path):
        df_old = pd.read_parquet(path)
        df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['Date', 'Ticker'])
    else:
        df_final = df_new
    df_final.to_parquet(path, index=False)
    log(f"💾 {len(df_new)} neue Anker im Buffer gesichert.")

if __name__ == "__main__":
    start_time = time.time()
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    if os.path.exists(ANCHOR_FILE):
        with open(ANCHOR_FILE, "r") as f: anchors = json.load(f)

    # Einmalig Heritage-Check am Anfang
    log("🩹 Starte initialen Heritage-Scan...")
    # (Heritage Code hier weggelassen für Übersicht, bleibt aber intern gleich)

    while (time.time() - start_time) < TOTAL_RUNTIME:
        loop_start = time.time()
        
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: current_pool = json.load(f)
        else: break

        new_anchors = process_pulse(current_pool)
        if new_anchors:
            save_data(pd.DataFrame(new_anchors))
            with open(ANCHOR_FILE, "w") as f: json.dump(anchors, f)

        elapsed = time.time() - loop_start
        wait = max(0, PULSE_INTERVAL - elapsed)
        
        if (time.time() - start_time) + PULSE_INTERVAL > TOTAL_RUNTIME:
            log("🏁 15 Minuten fast erreicht. Beende Zyklus für Sync...")
            break
            
        log(f"💤 Puls beendet. Warte {int(wait)}s bis zum nächsten Puls.")
        time.sleep(wait)
