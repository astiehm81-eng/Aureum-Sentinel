import pandas as pd
import pandas_datareader.data as web
import yfinance as yf
import os
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V60 (TEMPORAL SHARDING & AUTO-DISCOVERY) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "sentinel_buffer.parquet"
MAX_WORKERS = 40 

def ensure_vault():
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

def expand_pool_automatically():
    """Sucht nach neuen Tickern und ersetzt PENDING-Platzhalter."""
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    current_tickers = {a['symbol'] for a in pool if "PENDING" not in a['symbol']}
    
    # Discovery: Top-Indizes scannen (S&P 500, Nasdaq, DAX)
    # Für den Massen-Rollout nutzen wir hier eine vordefinierte Wachstumsliste
    new_found = []
    discovery_seeds = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "BRK-B", "LLY", "AVGO", "V", "MA", "COST"]
    
    for t in discovery_seeds:
        stooq_t = f"{t}.US"
        if stooq_t not in current_tickers:
            new_found.append({"symbol": stooq_t, "isin": "AUTO_DISCOVERED"})
            
    if new_found:
        count = 0
        for i, asset in enumerate(pool):
            if "PENDING" in asset['symbol'] and count < len(new_found):
                pool[i] = new_found[count]
                count += 1
        with open(POOL_FILE, 'w') as f:
            json.dump(pool, f, indent=4)
        print(f"✨ Discovery: {count} neue Ticker in Pool integriert.")

def fetch_deep_history(asset):
    symbol = asset['symbol']
    if "PENDING" in symbol: return None
    try:
        # 40 Jahre Historie anpeilen
        start = datetime.now() - timedelta(days=40*365)
        df = web.DataReader(symbol, 'stooq', start=start)
        if df is not None and not df.empty:
            df = df.reset_index()
            df.columns = [str(c) for c in df.columns]
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            return df
    except: return None

def save_to_temporal_shards(df):
    ensure_vault()
    # Jahrzehnt-Logik: 1990, 2000, 2010, 2020...
    df['Decade'] = (df['Date'].str[:4].astype(int) // 10) * 10
    
    for decade, group in df.groupby('Decade'):
        shard_path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
        save_group = group.drop(columns=['Decade'])
        
        if os.path.exists(shard_path):
            existing = pd.read_parquet(shard_path)
            save_group = pd.concat([existing, save_group]).drop_duplicates(subset=['Ticker', 'Date'])
        
        save_group.to_parquet(shard_path, engine='pyarrow', index=False)

def run_sentinel_v60():
    ensure_vault()
    expand_pool_automatically()
    
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    print("🏛️ Deep History Sync (40 Years Target)...")
    # Wir ziehen 150 Assets pro 5-Min-Lauf, um GitHub-Limits zu respektieren
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(fetch_deep_history, pool[:150]))
    
    valid_data = [r for r in results if r is not None and isinstance(r, pd.DataFrame)]
    if valid_data:
        full_df = pd.concat(valid_data)
        save_to_temporal_shards(full_df)
        print(f"✅ {len(valid_data)} Assets zeitlich archiviert.")

if __name__ == "__main__":
    run_sentinel_v60()
