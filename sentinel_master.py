import pandas as pd
import yfinance as yf
import os, json, time, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V82 (GAP-AUDIT & CLEAN-SYNC) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
BLACKLIST_FILE = "dead_assets.json"
MAX_WORKERS = 25 
START_TIME = time.time()

def run_gap_audit(pool, blacklist):
    """Analysiert den Bestand und findet Lücken."""
    existing_tickers = set()
    if os.path.exists(HERITAGE_DIR):
        for f in glob.glob(f"{HERITAGE_DIR}/*.parquet"):
            try:
                df = pd.read_parquet(f, columns=['Ticker'])
                existing_tickers.update(df['Ticker'].unique())
            except: continue
    
    # Lücken: Im Pool, aber nicht im Archiv und nicht auf der Blacklist
    missing = [a for a in pool if a['symbol'] not in existing_tickers and a['symbol'] not in blacklist]
    return missing, existing_tickers

def fetch_and_fix(asset):
    """Versucht Daten zu holen und korrigiert Ticker-Formate."""
    sym = asset['symbol']
    # Versuche: Original, ohne .US, mit .DE (falls zutreffend)
    variants = [sym, sym.replace(".US", "")]
    
    for s in variants:
        try:
            ticker = yf.Ticker(s)
            df = ticker.history(period="max")
            if df is not None and not df.empty:
                df = df.reset_index()
                df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
                df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                df['Ticker'] = sym # Immer unter Original-ID speichern
                return df, None
        except: continue
    return None, sym

def run_v82():
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    blacklist = []
    if os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE, 'r') as f: blacklist = json.load(f)

    # 1. Audit & Gap-Analyse
    missing, archived = run_gap_audit(pool, blacklist)
    print(f"🔍 Audit: {len(archived)} im Vault, {len(missing)} fehlen.")

    # 2. Gezieltes Auffüllen (140 Sek Laufzeit)
    new_data = []
    new_dead = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_and_fix, a) for a in missing[:60]]
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

    # 4. Blacklist & Reporting
    if new_dead:
        blacklist = list(set(blacklist + new_dead))
        with open(BLACKLIST_FILE, 'w') as f: json.dump(blacklist, f)

    total_count = len(archived) + len(new_data)
    with open(HUMAN_REPORT, "w", encoding="utf-8") as f:
        f.write(f"🛡️ AUREUM SENTINEL V82\n📅 {datetime.now().strftime('%H:%M:%S')}\n" + 
                "="*30 + 
                f"\n📊 Abdeckung: {(total_count/len(pool))*100:.2f}%\n✅ Assets: {total_count}\n💀 Blacklist: {len(blacklist)}")

if __name__ == "__main__": run_v82()
