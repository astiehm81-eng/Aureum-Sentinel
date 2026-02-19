import pandas as pd
import pandas_datareader.data as web
import os, json, time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V66 (AUDITED DEEP SYNC) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
STATUS_FILE = "vault_health.json"
MAX_WORKERS = 60 
START_TIME = time.time()

def ensure_vault():
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

def audit_data(df):
    """Sinnhaftigkeits-Check: Filtert Ausreißer und API-Müll."""
    if df is None or df.empty: return None
    # Entferne krasse Ausreißer (z.B. Preisänderung > 500% an einem Tag oft Fehler)
    df = df.sort_values('Date')
    df['pct'] = df['Price'].pct_change().abs()
    clean_df = df[df['pct'] < 5].drop(columns=['pct'])
    return clean_df

def fetch_data(asset, mode="history"):
    symbol = asset['symbol']
    try:
        start = datetime.now() - timedelta(days=40*365 if mode=="history" else 2)
        df = web.DataReader(symbol, 'stooq', start=start)
        if df is not None and not df.empty:
            df = df.reset_index()
            df.columns = [str(c) for c in df.columns]
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            return audit_data(df)
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

def generate_report():
    """Erstellt die Status-Anzeige für den Heritage Pool."""
    report = {"last_sync": datetime.now().isoformat(), "shards": {}}
    if os.path.exists(HERITAGE_DIR):
        for f in os.listdir(HERITAGE_DIR):
            if f.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(HERITAGE_DIR, f))
                report["shards"][f] = {"assets": int(df['Ticker'].nunique()), "rows": len(df)}
    with open(STATUS_FILE, "w") as f:
        json.dump(report, f, indent=4)

def run_v66():
    ensure_vault()
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    offset = int((time.time() % 86400) / 300) * 150 % len(pool)
    print(f"📡 Auditor V66 aktiv bei Index {offset}...")

    while (time.time() - START_TIME) < 220: # Etwas kürzer für Puffer
        current_batch = pool[offset : offset + 100]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(lambda a: fetch_data(a, "history"), current_batch))
        
        valid = [r for r in results if r is not None]
        if valid: save_shards(pd.concat(valid))
        offset = (offset + 100) % len(pool)
        time.sleep(5)

    generate_report()
    print("✅ Audit & Sync beendet.")

if __name__ == "__main__":
    run_v66()
