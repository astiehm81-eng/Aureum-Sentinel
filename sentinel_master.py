import pandas as pd
import pandas_datareader.data as web
import time
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (EISERNER STANDARD V50) ---
HERITAGE_FILE = "sentinel_heritage.parquet"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 30  # Maximale Power für 10k Assets

def check_modules():
    """Validiert, ob alle High-Performance Module geladen sind."""
    try:
        import pyarrow
        import pandas_datareader
        print("✅ System-Check: Alle Module (pyarrow, stooq-reader) einsatzbereit.")
    except ImportError as e:
        print(f"❌ KRITISCH: Modul fehlt: {e}. Bitte requirements.txt prüfen!")

def build_10k_discovery():
    """Erstellt den Pool, falls er noch nicht existiert."""
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f: return json.load(f)
    
    print("🛰️ Discovery: Generiere Basis-Pool...")
    # Hier füllen wir mit echten Stooq-Tickern auf
    pool = [{"symbol": s, "isin": "AUTO"} for s in ["SAP.DE", "ENR.DE", "AAPL.US", "MSFT.US"]]
    # Wir fügen Platzhalter hinzu, die wir sukzessive mit echten Welt-Tickern ersetzen
    for i in range(len(pool), 10000):
        pool.append({"symbol": f"PENDING_{i}", "isin": "PENDING"})
    
    with open(POOL_FILE, 'w') as f:
        json.dump(pool, f, indent=4)
    return pool

def fetch_stooq_engine(asset):
    """Kern-Engine für den Stooq-Abruf."""
    symbol = asset['symbol']
    if "PENDING" in symbol: return None
    try:
        start = datetime.now() - timedelta(days=5*365) # 5 Jahre Historie
        df = web.DataReader(symbol, 'stooq', start=start)
        if not df.empty:
            df = df[['Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            return df
    except:
        return None

def main():
    check_modules()
    pool = build_10k_discovery()
    
    # Verarbeite 500 Assets pro Zyklus (Stooq Rate-Limit Schutz)
    target_list = [a for a in pool if "PENDING" not in a['symbol']][:500]
    
    print(f"🏛️ Starte parallele Heritage-Erweiterung ({len(target_list)} Assets)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(fetch_stooq_engine, target_list))
    
    valid_data = [r for r in results if r is not None]
    if valid_data:
        final_df = pd.concat(valid_data)
        # Speichern mit 'pyarrow' Engine für Speed
        if os.path.exists(HERITAGE_FILE):
            existing = pd.read_parquet(HERITAGE_FILE)
            final_df = pd.concat([existing, final_df]).drop_duplicates()
        final_df.to_parquet(HERITAGE_FILE, engine='pyarrow', compression='snappy')
        print(f"✅ Batch abgeschlossen. Heritage-Vault aktualisiert.")

if __name__ == "__main__":
    main()
