import pandas as pd
import pandas_datareader.data as web
import os
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V63 (GLOBAL BALANCED DISCOVERY) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 50 

def ensure_vault():
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

def force_global_pool_expansion():
    """Erzeugt eine gleichmäßige Weltmarkt-Abdeckung (10k+ Assets)."""
    # Markt-Suffixe für Stooq
    markets = {
        "USA": [".US"], # NYSE/NASDAQ
        "EUROPE": [".DE", ".UK", ".FR", ".CH", ".IT", ".PL"], 
        "ASIA": [".JP", ".HK"] # Tokio, Hong Kong
    }
    
    # Basis-Alphabete für Generierung
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    new_pool = []

    # 1. USA (~3500 Assets)
    for c1 in chars:
        for c2 in chars:
            if len(new_pool) < 3500:
                new_pool.append({"symbol": f"{c1}{c2}.US", "region": "USA"})

    # 2. EUROPA (~3500 Assets - verteilt auf Länder)
    for suffix in markets["EUROPE"]:
        for c1 in chars:
            if len(new_pool) < 7000:
                new_pool.append({"symbol": f"{c1}{suffix}", "region": "EU"})

    # 3. ASIEN (~3000 Assets)
    for suffix in markets["ASIA"]:
        for c1 in chars:
            for c2 in chars:
                if len(new_pool) < 10000:
                    new_pool.append({"symbol": f"{c1}{c2}{suffix}", "region": "ASIA"})

    with open(POOL_FILE, 'w') as f:
        json.dump(new_pool, f, indent=4)
    print(f"🌍 Global Balance: Pool auf {len(new_pool)} Assets (USA/EU/ASIA) gesetzt.")

def fetch_deep_history(asset):
    symbol = asset['symbol']
    try:
        # Ziel: 40 Jahre Historie
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
    # Sharding nach Jahrzehnten für Performance
    df['Decade'] = (df['Date'].str[:4].astype(int) // 10) * 10
    for decade, group in df.groupby('Decade'):
        shard_path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
        save_group = group.drop(columns=['Decade'])
        if os.path.exists(shard_path):
            existing = pd.read_parquet(shard_path)
            save_group = pd.concat([existing, save_group]).drop_duplicates(subset=['Ticker', 'Date'])
        save_group.to_parquet(shard_path, engine='pyarrow', index=False)

def run_sentinel_v63():
    ensure_vault()
    # Nur initial ausführen oder wenn Pool leer ist
    if not os.path.exists(POOL_FILE) or os.path.getsize(POOL_FILE) < 1000:
        force_global_pool_expansion()
    
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    # Dynamischer Offset (500 Assets pro Lauf)
    # Nutzt die Zeit, um durch den gesamten 10k Pool zu rotieren
    offset = (int(time.time() // 300) % 20) * 500
    current_batch = pool[offset:offset+500]
    
    print(f"🏛️ Global Sync (Batch {offset}-{offset+500})...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(fetch_deep_history, current_batch))
    
    valid_data = [r for r in results if r is not None and isinstance(r, pd.DataFrame)]
    if valid_data:
        full_df = pd.concat(valid_data)
        save_to_temporal_shards(full_df)
        print(f"✅ {len(valid_data)} Assets global archiviert.")

if __name__ == "__main__":
    run_sentinel_v63()
