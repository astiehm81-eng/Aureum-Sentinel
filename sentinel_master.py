import pandas as pd
import pandas_datareader.data as web
import time
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V56 (SHARDED HERITAGE) ---
HERITAGE_DIR = "heritage_vault" # Neuer Ordner für die Archiv-Dateien
BUFFER_FILE = "sentinel_buffer.parquet"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 50 

def ensure_vault():
    if not os.path.exists(HERITAGE_DIR):
        os.makedirs(HERITAGE_DIR)

def get_shard_path(symbol):
    """Ordnet jedem Asset eine Datei basierend auf dem Anfangsbuchstaben zu."""
    shard = symbol[0].upper() if symbol[0].isalpha() else "NUM"
    return os.path.join(HERITAGE_DIR, f"heritage_shard_{shard}.parquet")

def fetch_history_stooq(asset):
    symbol = asset['symbol']
    try:
        # Volle Historie: 10 Jahre
        start = datetime.now() - timedelta(days=10*365)
        df = web.DataReader(symbol, 'stooq', start=start)
        if df is not None and not df.empty:
            df = df.reset_index()
            df.columns = [str(c) for c in df.columns]
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            return df
    except: return None

def save_to_shards(results):
    ensure_vault()
    valid_data = [r for r in results if r is not None]
    if not valid_data: return

    full_df = pd.concat(valid_data)
    # Daten auf die verschiedenen Shards (Dateien) verteilen
    for ticker in full_df['Ticker'].unique():
        shard_path = get_shard_path(ticker)
        asset_data = full_df[full_df['Ticker'] == ticker]
        
        if os.path.exists(shard_path):
            existing_shard = pd.read_parquet(shard_path)
            asset_data = pd.concat([existing_shard, asset_data]).drop_duplicates(subset=['Ticker', 'Date'])
        
        asset_data.to_parquet(shard_path, engine='pyarrow', index=False)

def run_mass_sync():
    pool = get_extensive_pool() # Deine 10k Liste
    # In jedem 5-Min-Lauf ziehen wir die Historie für 200 neue Assets
    # (So füllt sich der Vault stetig ohne Timeouts)
    print("🚀 Mass-History Sync (Sharded Mode)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(fetch_history_stooq, pool[:200]))
    
    save_to_shards(results)
    print("✅ Shards im Vault aktualisiert.")

if __name__ == "__main__":
    run_mass_sync()
