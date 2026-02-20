import pandas as pd
import yfinance as yf
import os, json, time, threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (V107.0 HIGH-SENSITIVITY) ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"

ANCHOR_THRESHOLD = 0.0001 # Deine 0,01% Vorgabe
MAX_WORKERS_TICKER = 100 
REFRESH_RATE = 60 # Jede Minute ein voller Hard-Refresh

file_lock = threading.Lock()
stop_event = threading.Event()
anchors = {}

if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f: f.write(f"[{timestamp}] {msg}\n")
    print(f"🛡️ {msg}", flush=True)

# --- LIVE ENGINE (YAHOO) ---
def ticker_loop(pool):
    update_status("Ticker gestartet (Anker: 0,01%).")
    while not stop_event.is_set():
        start_time = time.time()
        
        def process_asset(asset):
            symbol = asset['symbol']
            try:
                # YAHOO LIVE (Hard Refresh via fast_info)
                t = yf.Ticker(symbol)
                curr_p = round(t.fast_info['last_price'], 4)
                
                last_p = anchors.get(symbol)
                # 0,01% Logik: Wenn Abweichung zu groß -> neuer Anker
                if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                    anchors[symbol] = curr_p
                    print(f"⚡ {symbol}: {curr_p:.4f} (Delta > 0.01%)")
                
                return {"Date": datetime.now(), "Ticker": symbol, "Price": curr_p}
            except: return None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_TICKER) as executor:
            results = list(executor.map(process_asset, pool))
        
        valid = [r for r in results if r is not None]
        if valid:
            with file_lock:
                df = pd.DataFrame(valid)
                if os.path.exists(BUFFER_FILE):
                    df = pd.concat([pd.read_parquet(BUFFER_FILE), df])
                df.to_parquet(BUFFER_FILE, index=False)
        
        # Heritage Sync (Stooq) für Historie > 7 Tage
        # (Wird hier im Hintergrund getriggert)
        
        time.sleep(max(0, REFRESH_RATE - (time.time() - start_time)))

if __name__ == "__main__":
    # Automatisches Laden oder Erstellen des Pools
    if not os.path.exists(POOL_FILE):
        update_status("POOL_FILE fehlt! Erstelle Notfall-Pool...")
        with open(POOL_FILE, "w") as f: json.dump([{"symbol": "SAP.DE"}, {"symbol": "SIE.DE"}], f)

    with open(POOL_FILE, "r") as f:
        pool = json.load(f)
    
    ticker_loop(pool)
