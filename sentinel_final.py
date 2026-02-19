import pandas as pd
import yfinance as yf
import time
import os
import json
from datetime import datetime
import io

BUFFER_FILE = "sentinel_buffer.parquet"
POOL_FILE = "isin_pool.json"
BATCH_SIZE = 50 

def update_isin_pool():
    """Füllt den Pool selbsttätig mit den wichtigsten globalen Assets"""
    if os.path.exists(POOL_FILE):
        return # Pool ist bereits initialisiert
        
    print("🔎 Initialisiere ISIN-Pool (Top 10.000)...")
    # Hier nutzen wir vordefinierte Ticker-Listen (Indizes)
    # Beispielhaft starten wir mit den großen Indizes:
    base_tickers = ["^GDAXI", "^NDX", "^GSPC", "^STOXX50E"] 
    # In der Vollversion ziehen wir hier 10.000 Ticker via Screener
    pool = [{"isin": "DE0007164600", "symbol": "SAP.DE"}, {"isin": "DE000ENER6Y0", "symbol": "ENR.DE"}]
    
    with open(POOL_FILE, 'w') as f:
        json.dump(pool, f)

def run_sentinel_collector():
    update_isin_pool()
    with open(POOL_FILE, 'r') as f:
        pool = json.load(f)
    
    symbols = [s['symbol'] for s in pool]
    isin_map = {s['symbol']: s['isin'] for s in pool}
    
    minute_buffer = []
    last_save = time.time()

    print(f"🚀 Sammler aktiv für {len(symbols)} Assets. Modus: Yahoo-Live.")

    while True:
        cycle_start = time.time()
        
        # BATCH-DOWNLOAD (Jede Minute)
        try:
            # Wir ziehen alle Symbole des Pools in Batches
            for i in range(0, len(symbols), BATCH_SIZE):
                batch = symbols[i:i+BATCH_SIZE]
                data = yf.download(batch, period="1d", interval="1m", progress=False, group_by='ticker').tail(1)
                
                for sym in batch:
                    # Sicherstellen, dass wir den Preis extrahieren (Multi-Index Handling)
                    try:
                        price = data[sym]['Close'].iloc[0]
                        if not pd.isna(price):
                            minute_buffer.append({
                                'Timestamp': data.index[0].tz_localize(None),
                                'Price': float(price),
                                'ISIN': isin_map[sym]
                            })
                    except: continue
        except Exception as e:
            print(f"⚠️ API-Fehler: {e}")

        # SCHREIB-CHECK (Alle 15 Minuten)
        if time.time() - last_save >= 900: # 900 Sek = 15 Min
            if minute_buffer:
                new_df = pd.DataFrame(minute_buffer)
                if os.path.exists(BUFFER_FILE):
                    master = pd.read_parquet(BUFFER_FILE)
                    pd.concat([master, new_df]).drop_duplicates().to_parquet(BUFFER_FILE)
                else:
                    new_df.to_parquet(BUFFER_FILE)
                
                print(f"💾 {datetime.now().strftime('%H:%M')}: Block gesichert. Buffer geleert.")
                minute_buffer = [] 
                last_save = time.time()

        # Warten bis zur nächsten Minute
        sleep_time = 60 - (time.time() - cycle_start)
        if sleep_time > 0:
            time.sleep(sleep_time)

if __name__ == "__main__":
    run_sentinel_collector()
