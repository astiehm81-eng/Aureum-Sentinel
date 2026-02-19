import pandas as pd
import pandas_datareader.data as web
import time
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V52 (RAPID-CYCLE) ---
HERITAGE_FILE = "sentinel_heritage.parquet"
BUFFER_FILE = "sentinel_buffer.parquet"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 50  # Erhöht für massives Multithreading
CYCLE_MINUTES = 4 # Wir sammeln 4 Min, um 1 Min Puffer für den Push zu haben

def get_pool():
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f: return json.load(f)
    return []

def fetch_data_engine(asset, mode="heritage"):
    symbol = asset['symbol']
    try:
        if mode == "heritage":
            start = datetime.now() - timedelta(days=5*365)
            df = web.DataReader(symbol, 'stooq', start=start)
        else: # Live-Mode
            df = web.DataReader(symbol, 'stooq')
        
        if not df.empty:
            df = df[['Close']].iloc[:1].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            df['Timestamp'] = datetime.now()
            return df
    except: return None

def run_rapid_cycle():
    pool = get_pool()
    if not pool: return
    
    # 1. HERITAGE TURBO (Verarbeite die nächsten 1000 Assets parallel)
    # Wir suchen Assets, die noch nicht in der Heritage sind
    target_heritage = pool[:1000] 
    print(f"🚀 Heritage-Turbo: Sync für {len(target_heritage)} Assets...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        h_results = list(executor.map(lambda a: fetch_data_engine(a, "heritage"), target_heritage))
    
    # 2. LIVE-COLLECTOR (Sammelt jede Minute Daten für 4 Minuten)
    live_samples = []
    print(f"⏱️ Live-Check gestartet für {CYCLE_MINUTES} Minuten...")
    for i in range(CYCLE_MINUTES):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            l_results = list(executor.map(lambda a: fetch_data_engine(a, "live"), pool[:50])) # Top Assets
        live_samples.extend([r for r in l_results if r is not None])
        if i < CYCLE_MINUTES - 1: time.sleep(60)

    # 3. DATEN-SICHERUNG (PARALLEL & SNAPPY)
    save_data(h_results, live_samples)

def save_data(h_res, l_res):
    # Heritage speichern
    h_data = [r for r in h_res if r is not None]
    if h_data:
        df_h = pd.concat(h_data).reset_index()
        if os.path.exists(HERITAGE_FILE):
            df_h = pd.concat([pd.read_parquet(HERITAGE_FILE), df_h]).drop_duplicates(subset=['Ticker', 'Date'])
        df_h.to_parquet(HERITAGE_FILE, engine='pyarrow', compression='snappy')

    # Buffer speichern
    if l_res:
        df_l = pd.concat(l_res)
        if os.path.exists(BUFFER_FILE):
            df_l = pd.concat([pd.read_parquet(BUFFER_FILE), df_l])
        df_l.to_parquet(BUFFER_FILE, engine='pyarrow', compression='snappy')
    
    print(f"✅ Zyklus abgeschlossen. Heritage & Buffer synchronisiert.")

if __name__ == "__main__":
    run_rapid_cycle()
