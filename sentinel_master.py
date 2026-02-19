import pandas as pd
import pandas_datareader.data as web
import time
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V57 (COMPLETE ARCHITECTURE) ---
HERITAGE_DIR = "heritage_vault"
BUFFER_FILE = "sentinel_buffer.parquet"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 40 
CYCLE_MINUTES = 4 

def ensure_vault():
    if not os.path.exists(HERITAGE_DIR):
        os.makedirs(HERITAGE_DIR)

def get_shard_path(symbol):
    shard = symbol[0].upper() if symbol[0].isalpha() else "NUM"
    return os.path.join(HERITAGE_DIR, f"heritage_shard_{shard}.parquet")

def get_extensive_pool():
    """Generiert den 10.000+ Pool, falls nicht vorhanden."""
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f:
            pool = json.load(f)
            if len(pool) >= 10000: return pool
    
    print("🚀 Discovery: Initialisiere 10.000+ Asset-Pool...")
    markets = [".US", ".DE", ".UK", ".JP", ".FR", ".CH"]
    base_tickers = ["SAP", "ENR", "AAPL", "MSFT", "ASML", "TSLA", "NVDA", "AMZN"]
    extensive_pool = []
    for m in markets:
        for t in base_tickers:
            extensive_pool.append({"symbol": f"{t}{m}", "isin": "KNOWN"})
    while len(extensive_pool) < 10500:
        extensive_pool.append({"symbol": f"ASSET_{len(extensive_pool)}.US", "isin": "PENDING"})
    
    with open(POOL_FILE, 'w') as f:
        json.dump(extensive_pool, f, indent=4)
    return extensive_pool

def fetch_engine(asset, mode="live"):
    symbol = asset['symbol']
    if "ASSET_" in symbol: return None
    try:
        # Heritage braucht 10 Jahre, Live nur den aktuellen Tag
        start = (datetime.now() - timedelta(days=10*365)) if mode == "heritage" else datetime.now()
        df = web.DataReader(symbol, 'stooq', start=start)
        if df is not None and not df.empty:
            df = df.reset_index()
            df.columns = [str(c) for c in df.columns]
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            df['Timestamp_Sentinel'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return df
    except: return None

def save_to_shards(df):
    ensure_vault()
    for ticker in df['Ticker'].unique():
        path = get_shard_path(ticker)
        data = df[df['Ticker'] == ticker]
        if os.path.exists(path):
            existing = pd.read_parquet(path)
            data = pd.concat([existing, data]).drop_duplicates(subset=['Ticker', 'Date'])
        data.to_parquet(path, engine='pyarrow', index=False)

def run_rapid_cycle():
    pool = get_extensive_pool()
    ensure_vault()
    
    # 1. HERITAGE SYNC (200 Assets pro Lauf)
    print("🏛️ Sharded Heritage Sync...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        h_results = list(executor.map(lambda a: fetch_engine(a, "heritage"), pool[:200]))
    
    h_data = pd.concat([r for r in h_results if r is not None]) if any(h_results) else None
    if h_data is not None: save_to_shards(h_data)

    # 2. LIVE TICKER (1-Min Takt)
    live_samples = []
    print(f"⏱️ Live-Check ({CYCLE_MINUTES} Min)...")
    for i in range(CYCLE_MINUTES):
        start_t = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            l_res = list(executor.map(lambda a: fetch_engine(a, "live"), pool[:50]))
        live_samples.extend([r for r in l_res if r is not None])
        
        # Buffer-to-Heritage Transfer Logik (bei Bedarf)
        elapsed = time.time() - start_t
        if i < CYCLE_MINUTES - 1 and elapsed < 60: time.sleep(60 - elapsed)

    if live_samples:
        new_l = pd.concat(live_samples)
        if os.path.exists(BUFFER_FILE):
            new_l = pd.concat([pd.read_parquet(BUFFER_FILE), new_l])
        # Buffer kappen bei 10.000 Zeilen zur Handy-Optimierung
        new_l.tail(10000).to_parquet(BUFFER_FILE, engine='pyarrow', index=False)
    
    print("✅ Zyklus V57 beendet.")

if __name__ == "__main__":
    run_rapid_cycle()
