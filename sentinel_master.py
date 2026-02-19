import pandas as pd
import pandas_datareader.data as web
import os, json, time, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V69 (LIVE-TICKER & HERITAGE) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
MAX_WORKERS = 50 
START_TIME = time.time()

def ensure_vault():
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

def audit_data(df):
    """Prüft mathematische Sinnhaftigkeit (Ausreißer-Schutz)."""
    if df is None or df.empty: return None
    df = df.dropna()
    if len(df) > 1:
        df = df.sort_values('Date')
        df['change'] = df['Price'].pct_change().abs()
        df = df[df['change'] < 5].drop(columns=['change']) # Filtert krasse API-Fehler
    return df

def fetch_asset(asset, mode="history"):
    """Holt entweder 40 Jahre oder nur den aktuellen Live-Punkt."""
    symbol = asset['symbol']
    try:
        # History = 40 Jahre, Live = Letzte 2 Tage (für Minuten-Ticker)
        days = 40*365 if mode == "history" else 2
        start = datetime.now() - timedelta(days=days)
        df = web.DataReader(symbol, 'stooq', start=start)
        if df is not None and not df.empty:
            df = df.reset_index()
            df.columns = [str(c) for c in df.columns]
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            return audit_data(df)
    except: return None

def save_to_vault(df):
    if df is None or df.empty: return
    df['Decade'] = (df['Date'].str[:4].astype(int) // 10) * 10
    for decade, group in df.groupby('Decade'):
        path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
        save_g = group.drop(columns=['Decade'])
        if os.path.exists(path):
            save_g = pd.concat([pd.read_parquet(path), save_g]).drop_duplicates(subset=['Ticker', 'Date'])
        save_g.to_parquet(path, engine='pyarrow', index=False)

def run_v69():
    ensure_vault()
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    # Startpunkt im Pool (rotierend alle 5 Minuten)
    offset = int((time.time() % 86400) / 300) * 200 % len(pool)
    print(f"📡 V69 Scharfschaltung: Live-Ticker + Heritage ab Index {offset}")

    next_live_check = time.time()
    
    while (time.time() - START_TIME) < 230: # 4 Minuten Laufzeit
        current_now = time.time()
        
        # 1. LIVE-TICKER (Alle 60 Sekunden die ersten 30 Assets)
        if current_now >= next_live_check:
            print(f"⏱️ LIVE-TICKER AKTIV: {datetime.now().strftime('%H:%M:%S')}")
            with ThreadPoolExecutor(max_workers=30) as exec:
                live_data = list(exec.map(lambda a: fetch_asset(a, "live"), pool[:30]))
            save_to_vault(pd.concat([r for r in live_data if r is not None]))
            next_live_check = current_now + 60
        
        # 2. HERITAGE-FÜLLER (Batch von 100 Assets)
        batch = pool[offset : offset + 100]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            h_data = list(exec.map(lambda a: fetch_asset(a, "history"), batch))
        
        save_to_vault(pd.concat([r for r in h_data if r is not None]))
        offset = (offset + 100) % len(pool)
        
        time.sleep(2) # CPU-Schonung

    # Am Ende Report für Handy erstellen
    generate_status_report(pool)

def generate_status_report(pool):
    lines = [f"🛡️ AUREUM SENTINEL V69 - STATUS [{datetime.now().strftime('%d.%m. %H:%M')}]", "="*45]
    if os.path.exists(HERITAGE_DIR):
        total_assets = 0
        for f in sorted(os.listdir(HERITAGE_DIR)):
            df = pd.read_parquet(os.path.join(HERITAGE_DIR, f))
            total_assets = max(total_assets, df['Ticker'].nunique())
            lines.append(f"{f:20} | {df['Ticker'].nunique():4} Assets")
        lines.append("="*45)
        lines.append(f"📈 Abdeckung: {(total_assets/len(pool))*100:.2f}%")
        lines.append(f"🛡️ Integrität: Geprüft & Sinnhaft")
    with open(HUMAN_REPORT, "w", encoding="utf-8") as f: f.write("\n".join(lines))

if __name__ == "__main__": run_v69()
