import pandas as pd
import yfinance as yf
import os
import json
import time
import threading
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION (V108.8 - FORCE LOGGING) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
ANCHOR_FILE = "anchors_memory.json"
STATUS_FILE = "vault_status.txt"
SNAPSHOT_FILE = "AUREUM_SNAPSHOT.txt"

ANCHOR_THRESHOLD = 0.0005  
REFRESH_RATE = 300         
RUNTIME_LIMIT = 850 # Etwas kürzer als der Workflow-Timeout

db_lock = threading.Lock()
log_lock = threading.Lock()
anchors = {}
run_stats = {"anchors_set": 0, "stooq_updates": 0, "processed": 0}

def thread_safe_log(msg, end="\n"):
    """Erzwingt die sofortige Ausgabe im GitHub Action Log."""
    with log_lock:
        sys.stdout.write(f"{msg}{end}")
        sys.stdout.flush()

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f:
        f.write(f"[{timestamp}] {msg}\n")
    thread_safe_log(f"🛡️ {msg}")

def ticker_engine():
    start_time = time.time()
    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f:
                current_pool = json.load(f)
        else:
            current_pool = []
            
        thread_safe_log(f"\n--- NEUER PULS STARTET (Pool: {len(current_pool)} Assets) ---")
        run_stats["processed"] = 0
        results = []

        def process_asset(asset):
            symbol = asset['symbol']
            try:
                t = yf.Ticker(symbol)
                df = t.history(period="7d", interval="1m")
                if not df.empty:
                    curr_p = round(df['Close'].iloc[-1], 4)
                    last_p = anchors.get(symbol)
                    
                    with db_lock:
                        run_stats["processed"] += 1
                        if run_stats["processed"] % 5 == 0:
                            thread_safe_log(f"  [PULS] Fortschritt: {run_stats['processed']}/{len(current_pool)}", end="\r")

                    if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                        anchors[symbol] = curr_p
                        thread_safe_log(f"\n⚡ ANKER GESETZT: {symbol} @ {curr_p}")
                        return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": curr_p}
            except Exception as e:
                pass
            return None

        if current_pool:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_asset, a) for a in current_pool]
                for future in as_completed(futures):
                    res = future.result()
                    if res: results.append(res)

        if results:
            run_stats["anchors_set"] += len(results)
            save_to_vault(pd.DataFrame(results), "live_buffer")
            with open(ANCHOR_FILE, "w") as f:
                json.dump(anchors, f)
        
        thread_safe_log(f"\n✅ Puls abgeschlossen. Neue Anker: {len(results)}")
        elapsed = time.time() - loop_start
        time.sleep(max(10, REFRESH_RATE - elapsed))

def heritage_healer():
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, "r") as f:
        pool = json.load(f)

    thread_safe_log(f"Engine B: Starte Heritage-Scan für {len(pool)} Assets.")

    def heal(asset):
        symbol = asset['symbol']
        path = os.path.join(HERITAGE_DIR, f"{symbol}_history.parquet")
        
        if not os.path.exists(path) or (time.time() - os.path.getmtime(path)) > 86400:
            try:
                t = yf.Ticker(symbol)
                df_hist = t.history(period="max")
                if not df_hist.empty:
                    cutoff = datetime.now() - timedelta(days=7)
                    df_hist = df_hist[df_hist.index < cutoff]
                    df_hist['Ticker'] = symbol
                    df_hist.to_parquet(path)
                    thread_safe_log(f"🩹 HERITAGE REPAIRED: {symbol}")
                    return True
            except: pass
        return False

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(heal, a) for a in pool]
        for future in as_completed(futures):
            if future.result():
                run_stats["stooq_updates"] += 1

def save_to_vault(df_new, name):
    with db_lock:
        path = os.path.join(HERITAGE_DIR, f"{name}.parquet")
        if os.path.exists(path):
            df_old = pd.read_parquet(path)
            df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['Date', 'Ticker'], keep='last')
        else:
            df_final = df_new
        df_final.sort_values(['Ticker', 'Date']).to_parquet(path, index=False)

if __name__ == "__main__":
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    if os.path.exists(ANCHOR_FILE):
        with open(ANCHOR_FILE, "r") as f: anchors = json.load(f)
    
    update_status("Systemstart V108.8 - Eiserner Standard")
    
    # Engine B synchron vorab, damit Logs sauber getrennt sind
    heritage_healer()
    # Engine A übernimmt den Long-Run
    ticker_engine()
