import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
import os
import json
import sys

# Konfiguration
MASTER_VAULT = "sentinel_vault.parquet"
POOL_FILE = "isin_pool.json"

def fetch_worker_batch(batch_assets):
    """Holt Daten für einen spezifischen Batch aus dem Pool"""
    results = []
    
    # ThreadPool für maximale Geschwindigkeit innerhalb des Workers
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_asset = {executor.submit(get_single_quote, a): a for a in batch_assets}
        for future in future_to_asset:
            res = future.result()
            if res: results.append(res)
    return results

def get_single_quote(asset):
    try:
        # Nutzung des 1-Minuten-Intervalls (Eiserner Standard)
        ticker = yf.Ticker(asset['symbol'])
        df = ticker.history(period="1d", interval="1m").tail(1)
        if not df.empty:
            return {
                'Timestamp': df.index[0].tz_localize(None),
                'Price': float(df['Close'].iloc[0]),
                'ISIN': asset['isin'],
                'Source': 'Yahoo_Live_1m'
            }
    except: return None

def run():
    # 1. Pool laden
    with open(POOL_FILE, 'r') as f:
        full_pool = json.load(f)

    # 2. Segmentierung (Jeder Worker bekommt via CLI-Argument seinen Teil)
    # Beispiel: python sentinel_worker.py 0 100 (bearbeitet die ersten 100)
    start_idx = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    end_idx = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    
    my_batch = full_pool[start_idx:end_idx]
    print(f"👷 Worker übernimmt ISIN-Pool Segment {start_idx} bis {end_idx}")

    # 3. Daten sammeln
    new_data_list = fetch_worker_batch(my_batch)
    new_df = pd.DataFrame(new_data_list)

    # 4. Atomic Write (Verhindert Datei-Korruption)
    if not new_df.empty:
        temp_file = f"temp_batch_{start_idx}.parquet"
        new_df.to_parquet(temp_file)
        print(f"💾 Batch in {temp_file} zwischengespeichert.")

if __name__ == "__main__":
    run()
