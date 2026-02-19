import pandas as pd
import pandas_datareader.data as web
import os, json, time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V65 (LIVE-TICKER & DEEP SYNC) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 60 
START_TIME = time.time()

def ensure_vault():
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

def fetch_data(asset, mode="history"):
    symbol = asset['symbol']
    try:
        # Deep History (40J) oder Live (1T)
        days = 40*365 if mode == "history" else 2
        start = datetime.now() - timedelta(days=days)
        df = web.DataReader(symbol, 'stooq', start=start)
        if df is not None and not df.empty:
            df = df.reset_index()
            df.columns = [str(c) for c in df.columns]
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            return df
    except: return None

def save_shards(df):
    ensure_vault()
    df['Decade'] = (df['Date'].str[:4].astype(int) // 10) * 10
    for decade, group in df.groupby('Decade'):
        path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
        save_group = group.drop(columns=['Decade'])
        if os.path.exists(path):
            existing = pd.read_parquet(path)
            save_group = pd.concat([existing, save_group]).drop_duplicates(subset=['Ticker', 'Date'])
        save_group.to_parquet(path, engine='pyarrow', index=False)

def run_sentinel_v65():
    ensure_vault()
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    # Rotiert dynamisch durch den Pool (alle 5 Min ein neuer Startpunkt)
    offset = int((time.time() % 86400) / 300) * 150 % len(pool)
    print(f"📡 V65 Master-Clock: Deep-Sync ab Index {offset}")

    while (time.time() - START_TIME) < 240: # 4 Minuten Limit
        # 1. LIVE-CHECK (Top 20 Assets jede Minute)
        print(f"⏱️ Live-Check bei Sekunde {int(time.time()-START_TIME)}...")
        with ThreadPoolExecutor(max_workers=20) as live_exec:
            live_results = list(live_exec.map(lambda a: fetch_data(a, "live"), pool[:20]))
        
        # 2. DEEP-HISTORY BATCH (Nächste 100 Assets)
        current_batch = pool[offset : offset + 100]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as history_exec:
            h_results = list(history_exec.map(lambda a: fetch_data(a, "history"), current_batch))
        
        # Speichern
        all_valid = [r for r in h_results + live_results if r is not None and isinstance(r, pd.DataFrame)]
        if all_valid:
            save_shards(pd.concat(all_valid))
        
        offset = (offset + 100) % len(pool)
        time.sleep(10) # Kurze Verschnaufpause für die API

    print("🏁 Zyklus beendet. Bereite Git-Push vor.")

if __name__ == "__main__":
    run_sentinel_v65()
