import pandas as pd
import yfinance as yf
import os
import json
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION (V108.6 - MAX VISIBILITY) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
ANCHOR_FILE = "anchors_memory.json"
STATUS_FILE = "vault_status.txt"
SNAPSHOT_FILE = "AUREUM_SNAPSHOT.txt"

# --- STRATEGIE-VORGABEN ---
ANCHOR_THRESHOLD = 0.0005  # 0,05%
REFRESH_RATE = 300         
RUNTIME_LIMIT = 900        
MAX_WORKERS = 100          

db_lock = threading.Lock()
anchors = {}
run_stats = {"anchors_set": 0, "stooq_updates": 0, "processed": 0}

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"🛡️ {msg}", flush=True)

# --- ENGINE A: LIVE-PULS MIT ECHTEM FORTSCHRITT ---
def ticker_engine(pool):
    start_time = time.time()
    update_status("Engine A: Yahoo Live-Puls (0,05% Anker) gestartet.")
    
    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        run_stats["processed"] = 0
        current_batch_anchors = 0
        
        def process_asset(asset):
            symbol = asset['symbol']
            try:
                t = yf.Ticker(symbol)
                df = t.history(period="7d", interval="1m")
                if not df.empty:
                    curr_p = round(df['Close'].iloc[-1], 4)
                    last_p = anchors.get(symbol)
                    
                    # Live-Ticker im Terminal (Fortschrittsanzeige)
                    run_stats["processed"] += 1
                    if run_stats["processed"] % 10 == 0:
                        print(f"  [PULS] Verarbeite {run_stats['processed']}/{len(pool)} | Letzter: {symbol} @ {curr_p}      ", end="\r", flush=True)

                    if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                        anchors[symbol] = curr_p
                        return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": curr_p}
            except: pass
            return None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_asset, a) for a in pool]
            results = []
            for future in as_completed(futures):
                res = future.result()
                if res:
                    results.append(res)
                    print(f"\n⚡ ANKER GESETZT: {res['Ticker']} @ {res['Price']} (Delta > 0.05%)", flush=True)

        if results:
            run_stats["anchors_set"] += len(results)
            save_to_vault(pd.DataFrame(results), "live_buffer")
            with open(ANCHOR_FILE, "w") as f:
                json.dump(anchors, f)
        
        print(f"\n✅ Loop beendet. {len(results)} neue Anker. Warte auf nächsten Puls...", flush=True)
        elapsed = time.time() - loop_start
        time.sleep(max(10, REFRESH_RATE - elapsed))

# --- ENGINE B: HERITAGE MIT LIVE-FEEDBACK ---
def heritage_healer(pool):
    update_status("Engine B: Stooq-Verheiratung gestartet.")
    
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
                    print(f"🩹 HERITAGE HEALED: {symbol} (Historie importiert)", flush=True)
                    return True
            except: pass
        return False

    with ThreadPoolExecutor(max_workers=30) as executor:
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

def generate_snapshot(pool):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(SNAPSHOT_FILE, "w") as f:
        f.write(f"--- AUREUM DATABASE SNAPSHOT (V108.6) ---\n")
        f.write(f"Zeitpunkt:      {timestamp}\n")
        f.write(f"Pool-Größe:     {len(pool)} Assets\n")
        f.write(f"Neue Anker:     {run_stats['anchors_set']}\n")
        f.write(f"Stooq-Heilung:  {run_stats['stooq_updates']} Assets\n")
        f.write(f"Status:         SYSTEM ONLINE\n")

if __name__ == "__main__":
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    if os.path.exists(ANCHOR_FILE):
        with open(ANCHOR_FILE, "r") as f: anchors = json.load(f)
    with open(POOL_FILE, "r") as f: pool = json.load(f)

    # Start Heritage Healer
    h_thread = threading.Thread(target=heritage_healer, args=(pool,), daemon=True)
    h_thread.start()
    
    ticker_engine(pool)
    generate_snapshot(pool)
    update_status("Zyklus beendet.")
