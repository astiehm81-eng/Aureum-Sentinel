import pandas as pd
import pandas_datareader.data as web
import os, json, time, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V76 (RELIABLE ENGINE) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
MAX_WORKERS = 45 # Optimale Balance für GitHub RAM
START_TIME = time.time()

def absolute_cleanup():
    """Löscht aktiv alle Dateileichen im Root."""
    garbage = ["sentinel_*.csv", "sentinel_*.parquet", "vault_health.json", "requirements.txt"]
    for pattern in garbage:
        for f in glob.glob(pattern):
            try: os.remove(f)
            except: pass
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

def fetch_asset(asset, mode="history"):
    symbol = asset['symbol']
    try:
        days = 40*365 if mode=="history" else 3
        start = datetime.now() - timedelta(days=days)
        df = web.DataReader(symbol, 'stooq', start=start)
        if df is not None and not df.empty:
            df = df.reset_index()
            df.columns = [str(c) for c in df.columns]
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            # Auditor (Mathematische Sinnhaftigkeit)
            if len(df) > 5:
                df = df.sort_values('Date')
                df['pct'] = df['Price'].pct_change().abs()
                df = df[df['pct'] < 5].drop(columns=['pct'])
            return df
    except: return None

def save_to_shards(df):
    if df is None or df.empty: return
    df['Decade'] = (df['Date'].str[:4].astype(int) // 10) * 10
    for decade, group in df.groupby('Decade'):
        path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
        new_data = group.drop(columns=['Decade'])
        if os.path.exists(path):
            try:
                existing = pd.read_parquet(path)
                new_data = pd.concat([existing, new_data]).drop_duplicates(subset=['Ticker', 'Date'])
            except: pass
        new_data.to_parquet(path, engine='pyarrow', index=False)

def run_v76():
    absolute_cleanup()
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    # Rotation basierend auf der Zeit
    offset = int((time.time() % 86400) / 300) * 400 % len(pool)
    print(f"📡 V76 aktiv (Index {offset}). Safe-Run Modus.")

    next_live = time.time()
    # Wir begrenzen auf 240 Sekunden (4 Min), damit der Git-Sync danach sicher ist
    while (time.time() - START_TIME) < 240:
        now = time.time()
        # LIVE-TICKER (Priorität)
        if now >= next_live:
            with ThreadPoolExecutor(max_workers=20) as ex:
                res = [r for r in ex.map(lambda a: fetch_asset(a, "live"), pool[:30]) if r is not None]
            if res: save_to_shards(pd.concat(res))
            next_live = now + 60
        
        # MASSEN-SYNC (Batch 120 für hohen Durchsatz)
        batch = pool[offset : offset + 120]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            h_results = [r for r in ex.map(lambda a: fetch_asset(a, "history"), batch) if r is not None]
        
        if h_results: save_to_shards(pd.concat(h_results))
        offset = (offset + 120) % len(pool)
        time.sleep(1)

    # REPORTING
    generate_report(pool)
    print("✅ Zyklus V76 abgeschlossen.")

def generate_report(pool):
    lines = [f"🛡️ AUREUM SENTINEL V76", f"📅 {datetime.now().strftime('%H:%M:%S')}", "="*40]
    total_assets = 0
    if os.path.exists(HERITAGE_DIR):
        for f in sorted(os.listdir(HERITAGE_DIR)):
            if f.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(HERITAGE_DIR, f))
                total_assets = max(total_assets, df['Ticker'].nunique())
                lines.append(f"• {f:18} | {df['Ticker'].nunique():4} Assets")
    lines.append("="*40)
    lines.append(f"📊 Abdeckung: {(total_assets/len(pool))*100:.2f}% | Status: AKTIV")
    with open(HUMAN_REPORT, "w", encoding="utf-8") as f: f.write("\n".join(lines))

if __name__ == "__main__": run_v76()
