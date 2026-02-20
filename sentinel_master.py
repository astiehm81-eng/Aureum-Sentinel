import pandas as pd
import yfinance as yf
import os, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V100.6 (15-MIN-CYCLE-LOGIC) ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.json"
ANCHOR_THRESHOLD = 0.001
MAX_WORKERS = 15
RUNTIME_LIMIT = 780  # 13 Minuten Laufzeit

# ... [get_live_ticker_update & heal_gaps Funktionen wie in V100.5] ...

def run_v100_6():
    print(f"🛡️ V100.6 START | 15min Cycle Mode", flush=True)
    start_time = time.time()
    
    # Der Loop läuft nun exakt 13 Minuten
    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        
        # 1. Ganzen Pool verarbeiten (mit integrierter Gap-Heilung aus V100.5)
        # 2. 0,1% Anchor Check & Buffer Update
        
        # [Hier Aufruf der process_tick_v100_5 Logik]
        
        elapsed = time.time() - loop_start
        # Wir warten bis zur nächsten vollen Minute
        time.sleep(max(0, 60 - elapsed))

    print("🏁 13min erreicht. Beende für Git-Sync.", flush=True)

if __name__ == "__main__":
    run_v100_6()
