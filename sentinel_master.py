import pandas as pd
import pandas_datareader.data as web
import time
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (EISERNER STANDARD) ---
HERITAGE_FILE = "sentinel_heritage.parquet"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 20  # Parallelisierung für 10k Assets
ANCHOR_THRESHOLD = 0.001 # 0.1% Regel

def get_10k_pool():
    """Lädt oder generiert den massiven 10.000er Pool."""
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f: return json.load(f)
    
    # Platzhalter: Hier füllen wir die Liste mit den 10.000 Tickersymbolen
    # In der Praxis wird diese Liste hier injiziert
    pool = [{"symbol": f"ASSET_{i}.US", "isin": f"ISIN_{i}"} for i in range(10000)]
    with open(POOL_FILE, 'w') as f:
        json.dump(pool, f, indent=4)
    return pool

def fetch_stooq_data(asset):
    """Holt historische Daten von Stooq."""
    symbol = asset['symbol']
    try:
        # Abfrage von Stooq via Pandas Datareader
        df = web.DataReader(symbol, 'stooq')
        if not df.empty:
            df = df[['Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            return df
    except Exception:
        return None

def build_parallel_heritage(pool):
    """Parallelisierter Heritage-Build für maximale Performance."""
    print(f"🏛️ Starte parallelen Heritage-Build für {len(pool)} Assets...")
    
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Startet alle Abfragen gleichzeitig
        future_to_asset = {executor.submit(fetch_stooq_data, asset): asset for asset in pool[:500]} # Erste Tranche
        
        for future in future_to_asset:
            res = future.result()
            if res is not None:
                results.append(res)
    
    if results:
        new_heritage = pd.concat(results)
        # Speichern & Mergen mit bestehenden Daten
        if os.path.exists(HERITAGE_FILE):
            old_heritage = pd.read_parquet(HERITAGE_FILE)
            new_heritage = pd.concat([old_heritage, new_heritage]).drop_duplicates()
        new_heritage.to_parquet(HERITAGE_FILE, compression='snappy')
        print(f"✅ Heritage aktualisiert. Größe: {len(new_heritage)} Zeilen.")

if __name__ == "__main__":
    start_time = time.time()
    pool = get_10k_pool()
    build_parallel_heritage(pool)
    print(f"⏱️ Zyklus beendet in {round(time.time() - start_time, 2)} Sekunden.")
