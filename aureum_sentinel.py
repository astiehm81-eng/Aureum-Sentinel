import pandas as pd
import yfinance as yf
import os
import json
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION (V108.7 - DYNAMISCHES WACHSTUM) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
ANCHOR_FILE = "anchors_memory.json"
STATUS_FILE = "vault_status.txt"
SNAPSHOT_FILE = "AUREUM_SNAPSHOT.txt"

ANCHOR_THRESHOLD = 0.0005  
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

# --- ENGINE A: LIVE-PULS (DYNAMISCHER POOL-IMPORT) ---
def ticker_engine():
    start_time = time.time()
    update_status("Engine A: Start (0,05% Anker).")
    
    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        
        # WICHTIG: Pool bei jedem Puls neu laden, um Wachstum zu ermöglichen
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f:
                current_pool = json.load(f)
        else:
            current_pool = []
            
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
                    
                    run_stats["processed"] += 1
                    # Anzeige des Fortschritts im Terminal
                    print(f"  [PULS] {run_stats['processed']}/{len(current_pool)} | {symbol} @ {curr_p}      ", end="\r", flush=True)

                    if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                        anchors[symbol] = curr_p
                        return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": curr_p}
            except: pass
            return None

        if current_pool:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_asset, a) for a in current_pool]
                for future in as_completed(futures):
                    res = future.result()
                    if res:
                        results.append(res)
                        print(f"\n⚡ ANKER: {res['Ticker']} @ {res['Price']}", flush=True)

        if results:
            run_stats["anchors_set"] += len(results)
            save_to_vault(pd.DataFrame(results), "live_buffer")
            with open(ANCHOR_FILE, "w") as f:
                json.dump(anchors, f)
        
        print(f"\n✅ Puls beendet. Pool-Größe: {len(current_pool)} | Neue Anker: {len(results)}", flush=True)
        elapsed = time.time() - loop_start
        time.sleep(max(10, REFRESH_RATE - elapsed))

# --- ENGINE B: HERITAGE (YAHOO/STOOQ VERHEIRATUNG) ---
def heritage_healer():
    """Engine B füllt die historischen Lücken für alle Assets im Pool."""
    update_status("Engine B: Scan für Heritage-Lücken...")
    
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, "r") as f:
        pool = json.load(f)

    def heal(asset):
        symbol = asset['symbol']
        path = os.path.join(HERITAGE_DIR, f"{symbol}_history.parquet")
        
        # Nur heilen, wenn Datei fehlt oder älter als 24h ist
        if not os.path.exists(path) or (time.time() - os.path.getmtime(path)) > 86400:
            try:
                t = yf.Ticker(symbol)
                df_hist = t.history(period="max")
                if not df_hist.empty:
                    # Schnittstelle: Alles älter als 7 Tage wird weggeschrieben
                    cutoff = datetime.now() - timedelta(days=7)
                    df_hist = df_hist[df_hist.index < cutoff]
                    df_hist['Ticker'] = symbol
                    df_hist.to_parquet(path)
                    # EXPLIZITES LOGGING FÜR DICH:
                    print(f"🩹 HERITAGE REPAIRED: {symbol} (Archiv befüllt)", flush=True)
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

def generate_snapshot():
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(SNAPSHOT_FILE, "w") as f:
        f.write(f"--- AUREUM DATABASE SNAPSHOT (V108.7) ---\n")
        f.write(f"Zeitpunkt:      {timestamp}\n")
        f.write(f"Neue Anker:     {run_stats['anchors_set']}\n")
        f.write(f"Heritage-Heal:  {run_stats['stooq_updates']} Assets\n")
        f.write(f"Status:         WACHSTUMSAKTIV\n")

if __name__ == "__main__":
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    if os.path.exists(ANCHOR_FILE):
        with open(ANCHOR_FILE, "r") as f: anchors = json.load(f)
    
    # Engine B startet im Hintergrund
    h_thread = threading.Thread(target=heritage_healer, daemon=True)
    h_thread.start()
    
    ticker_engine()
    generate_snapshot()
