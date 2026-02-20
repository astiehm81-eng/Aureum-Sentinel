import pandas as pd
import yfinance as yf
import os, json, time, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V83 (TICKER TRANSLATOR) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
BLACKLIST_FILE = "dead_assets.json"
MAX_WORKERS = 30 
START_TIME = time.time()

# Yahoo-Suffix Map für saubere Abfragen
SUFFIX_MAP = {
    "DE": ".DE", # XETRA
    "US": "",    # NASDAQ/NYSE (kein Suffix)
    "FR": ".PA", # Paris
    "UK": ".L",  # London
    "JP": ".T",  # Tokyo
    "CH": ".SW"  # Schweiz
}

def clean_ticker(sym):
    """Übersetzt Asset-Namen in Yahoo-konforme Ticker."""
    # Entferne $ Zeichen aus deinem Log
    sym = sym.replace("$", "").strip()
    
    # Wenn ein Punkt vorhanden ist (z.B. AAPL.DE), suffix korrigieren
    if "." in sym:
        base, region = sym.split(".", 1)
        suffix = SUFFIX_MAP.get(region.upper(), f".{region}")
        return f"{base}{suffix}"
    
    return sym

def fetch_with_retry(asset):
    original_id = asset['symbol']
    target_ticker = clean_ticker(original_id)
    
    try:
        t = yf.Ticker(target_ticker)
        # Wir versuchen erst die Historie
        df = t.history(period="max")
        
        # Fallback: Wenn max nicht geht, versuche die letzten 5 Jahre
        if df.empty:
            df = t.history(period="5y")
            
        if not df.empty:
            df = df.reset_index()
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df['Ticker'] = original_id # Speichern unter deiner Pool-ID
            return df, None
    except Exception as e:
        pass
    
    return None, original_id

def run_v83():
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    blacklist = []
    if os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE, 'r') as f: blacklist = json.load(f)

    # 1. Audit: Was fehlt?
    existing_tickers = set()
    if os.path.exists(HERITAGE_DIR):
        for f in glob.glob(f"{HERITAGE_DIR}/*.parquet"):
            try:
                df = pd.read_parquet(f, columns=['Ticker'])
                existing_tickers.update(df['Ticker'].unique())
            except: continue

    missing = [a for a in pool if a['symbol'] not in existing_tickers and a['symbol'] not in blacklist]
    print(f"📡 V83: Starte Übersetzung für {len(missing)} Lücken.")

    # 2. Parallel Fetch mit korrigierter Syntax
    new_data = []
    new_dead = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        # Verarbeite 100 Assets pro Lauf
        futures = [ex.submit(fetch_with_retry, a) for a in missing[:100]]
        for f in futures:
            res, dead = f.result()
            if res is not None: new_data.append(res)
            elif dead: new_dead.append(dead)

    # 3. Speichern (Sharding)
    if new_data:
        full_df = pd.concat(new_data)
        full_df['Decade'] = (full_df['Date'].str[:4].astype(int) // 10) * 10
        for decade, group in full_df.groupby('Decade'):
            path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
            to_save = group.drop(columns=['Decade'])
            if os.path.exists(path):
                old = pd.read_parquet(path)
                to_save = pd.concat([old, to_save]).drop_duplicates(subset=['Ticker', 'Date'])
            to_save.to_parquet(path, engine='pyarrow', index=False)

    # 4. Blacklist-Schutz
    if new_dead:
        # Nur auf Blacklist, wenn es wirklich $ASSET_... Platzhalter sind
        blacklist.extend([d for d in new_dead if "ASSET_" in d])
        with open(BLACKLIST_FILE, 'w') as f: json.dump(list(set(blacklist)), f)

    # Status Report
    total = len(existing_tickers) + len(new_data)
    with open(HUMAN_REPORT, "w", encoding="utf-8") as f:
        f.write(f"🛡️ AUREUM SENTINEL V83\n📊 Abdeckung: {(total/len(pool))*100:.2f}%\n✅ Archiviert: {total}\n💀 Blacklist: {len(blacklist)}")

if __name__ == "__main__":
    run_v83()
