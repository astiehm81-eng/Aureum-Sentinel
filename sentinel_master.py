import pandas as pd
import pandas_datareader.data as web
import os, json, time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V64 (FULL 5-MIN UTILIZATION) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 60 
START_TIME = time.time()

def ensure_vault():
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

def fetch_data(asset, mode="history"):
    symbol = asset['symbol']
    try:
        # 40 Jahre für Historie, 1 Tag für Live
        days = 40*365 if mode == "history" else 1
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

def run_sentinel_v64():
    ensure_vault()
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    # OFFSET LOGIK: Wo fangen wir an?
    offset = int((time.time() % 86400) / 300) * 100 
    
    print(f"🚀 V64 Scharfschaltung: Starte Dauerlauf ab Index {offset}...")
    
    # DAUERLAUF: Solange wir unter 4 Minuten (240s) sind, machen wir weiter
    while (time.time() - START_TIME) < 240:
        batch_start = time.time()
        current_batch = pool[offset : offset + 100]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Parallel History Fetch
            results = list(executor.map(lambda a: fetch_data(a, "history"), current_batch))
        
        valid = [r for r in results if r is not None and isinstance(r, pd.DataFrame)]
        if valid:
            save_shards(pd.concat(valid))
            print(f"✅ Batch verarbeitet. {len(valid)} Assets gesichert. Zeit: {int(time.time()-START_TIME)}s")
        
        # Weiterspringen im Pool
        offset += 100
        if offset >= len(pool): offset = 0
        
        # Kurze Pause für den Live-Ticker Check (Tradegate-Sim)
        time.sleep(2) 

    print("🏁 5-Minuten-Limit fast erreicht. Beende Lauf für Git-Commit.")

if __name__ == "__main__":
    run_sentinel_v64()
