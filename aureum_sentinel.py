import pandas as pd
import yfinance as yf
import os
import json
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (V108.5 - STOOQ/YAHOO HYBRID) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
ANCHOR_FILE = "anchors_memory.json"
STATUS_FILE = "vault_status.txt"
SNAPSHOT_FILE = "AUREUM_SNAPSHOT.txt"

# --- STRATEGIE-VORGABEN ---
ANCHOR_THRESHOLD = 0.0005  # 0,05% Sensitivität
REFRESH_RATE = 300         # 5-Minuten Puls
RUNTIME_LIMIT = 900        # 15 Minuten Laufzeit pro Run
MAX_WORKERS = 100          # Maximale Parallelität

db_lock = threading.Lock()
anchors = {}
run_stats = {"anchors_set": 0, "stooq_updates": 0}

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"🛡️ {msg}", flush=True)

# --- ENGINE A: REZENTE DATEN (YAHOO: 1 WOCHE BIS JETZT) ---
def ticker_engine(pool):
    start_time = time.time()
    update_status("Engine A: Yahoo Live-Puls (0,05% Anker) gestartet.")
    
    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        
        def process_asset(asset):
            symbol = asset['symbol']
            try:
                t = yf.Ticker(symbol)
                # Letzte 7 Tage für die "jüngste Vergangenheit"
                df = t.history(period="7d", interval="1m")
                
                if not df.empty:
                    curr_p = round(df['Close'].iloc[-1], 4)
                    last_p = anchors.get(symbol)
                    
                    print(f"  [PULS] {symbol}: {curr_p}      ", end="\r")

                    if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                        anchors[symbol] = curr_p
                        return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": curr_p}
            except: pass
            return None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(process_asset, pool))
        
        valid = [r for r in results if r is not None]
        if valid:
            run_stats["anchors_set"] += len(valid)
            save_to_vault(pd.DataFrame(valid), "live_buffer")
            with open(ANCHOR_FILE, "w") as f:
                json.dump(anchors, f)
        
        elapsed = time.time() - loop_start
        time.sleep(max(10, REFRESH_RATE - elapsed))

# --- ENGINE B: HISTORIE (STOOQ: ALLES ÄLTER ALS 1 WOCHE) ---
def heritage_healer(pool):
    update_status("Engine B: Stooq-Verheiratung (Historie > 1 Woche).")
    def heal(asset):
        symbol = asset['symbol']
        path = os.path.join(HERITAGE_DIR, f"{symbol}_history.parquet")
        
        if not os.path.exists(path) or (time.time() - os.path.getmtime(path)) > 86400:
            try:
                t = yf.Ticker(symbol)
                df_hist = t.history(period="max")
                if not df_hist.empty:
                    # Filter: Nur Daten älter als 1 Woche in die Heritage-Vault
                    cutoff = datetime.now() - timedelta(days=7)
                    df_hist = df_hist[df_hist.index < cutoff]
                    df_hist['Ticker'] = symbol
                    df_hist.to_parquet(path)
                    return True
            except: pass
        return False

    with ThreadPoolExecutor(max_workers=30) as executor:
        run_stats["stooq_updates"] = sum(list(executor.map(heal, pool)))

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
        f.write(f"--- AUREUM DATABASE SNAPSHOT (V108.5) ---\n")
        f.write(f"Zeitpunkt:      {timestamp}\n")
        f.write(f"Pool-Größe:     {len(pool)} Assets\n")
        f.write(f"Strategie:      Stooq (Hist) + Yahoo (7d-Live)\n")
        f.write(f"Anker:          0,05% / 5 Min\n")
        f.write(f"Neue Anker:     {run_stats['anchors_set']}\n")
        f.write(f"Stooq-Heilung:  {run_stats['stooq_updates']} Assets\n")
        f.write(f"Status:         SYSTEM ONLINE\n")

if __name__ == "__main__":
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    if os.path.exists(ANCHOR_FILE):
        with open(ANCHOR_FILE, "r") as f: anchors = json.load(f)
    with open(POOL_FILE, "r") as f: pool = json.load(f)

    threading.Thread(target=heritage_healer, args=(pool,), daemon=True).start()
    ticker_engine(pool)
    generate_snapshot(pool)
    update_status("Zyklus beendet.")
