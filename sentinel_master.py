import pandas as pd
import yfinance as yf
import os, json, time, glob
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V86 (DEEP SCAN & LOGGING) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
BLACKLIST_FILE = "dead_assets.json"
TICKER_MAP_FILE = "ticker_mapping.json"
MAX_WORKERS = 10 # Weniger Threads = stabilere Verbindung
START_TIME = time.time()

def ensure_files():
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    if not os.path.exists(TICKER_MAP_FILE):
        with open(TICKER_MAP_FILE, 'w') as f: json.dump({}, f)
    if not os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE, 'w') as f: json.dump([], f)

def find_ticker_yf(query):
    """Nutzt die integrierte yfinance Suche."""
    try:
        # Säuberung des Symbols (Entferne $ und Regionen-Suffixe für die Suche)
        search_query = query.replace("$", "").split(".")[0]
        data = yf.Search(search_query, max_results=3).quotes
        if data:
            for quote in data:
                if quote.get('quoteType') in ['EQUITY', 'ETF']:
                    return quote['symbol']
    except Exception as e:
        print(f"⚠️ Suche fehlgeschlagen für {query}: {e}")
    return None

def process_asset(asset, ticker_map):
    orig_id = asset['symbol'].replace("$", "")
    print(f"🔎 Bearbeite: {orig_id}...")
    
    # 1. Ticker finden
    target_ticker = ticker_map.get(orig_id)
    if not target_ticker:
        target_ticker = find_ticker_yf(orig_id)
        if not target_ticker:
            return None, orig_id, None
    
    # 2. Daten laden
    try:
        t = yf.Ticker(target_ticker)
        df = t.history(period="10y") # Erstmal 10 Jahre für Schnelligkeit
        if not df.empty:
            df = df.reset_index()
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df['Ticker'] = orig_id
            print(f"✅ Daten gefunden für {orig_id} -> {target_ticker}")
            return df, None, {orig_id: target_ticker}
    except: pass
    
    return None, orig_id, None

def run_v86():
    ensure_files()
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    with open(TICKER_MAP_FILE, 'r') as f: ticker_map = json.load(f)
    with open(BLACKLIST_FILE, 'r') as f: blacklist = json.load(f)

    # Archiv-Check
    archived = set()
    for f in glob.glob(f"{HERITAGE_DIR}/*.parquet"):
        try: archived.update(pd.read_parquet(f, columns=['Ticker'])['Ticker'].unique())
        except: pass

    missing = [a for a in pool if a['symbol'].replace("$", "") not in archived and a['symbol'] not in blacklist]
    print(f"📡 V86 Start: {len(missing)} Lücken. Batch-Größe: 50.")

    new_data, new_dead, new_mappings = [], [], {}
    
    # Batch-Verarbeitung
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_asset, a, ticker_map) for a in missing[:50]]
        for f in futures:
            df, dead, mapping = f.result()
            if df is not None: new_data.append(df)
            if dead and not df: new_dead.append(dead)
            if mapping: new_mappings.update(mapping)

    # Speichern
    if new_data:
        full_df = pd.concat(new_data)
        full_df['Decade'] = (full_df['Date'].str[:4].astype(int) // 10) * 10
        for decade, group in full_df.groupby('Decade'):
            path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
            save_df = group.drop(columns=['Decade'])
            if os.path.exists(path):
                old = pd.read_parquet(path)
                save_df = pd.concat([old, save_df]).drop_duplicates(subset=['Ticker', 'Date'])
            save_df.to_parquet(path, engine='pyarrow', index=False)

    # Persistence
    if new_mappings:
        ticker_map.update(new_mappings)
        with open(TICKER_MAP_FILE, 'w') as f: json.dump(ticker_map, f, indent=4)
    if new_dead:
        blacklist = list(set(blacklist + new_dead))
        with open(BLACKLIST_FILE, 'w') as f: json.dump(blacklist, f)

    # Status
    with open(HUMAN_REPORT, "w", encoding="utf-8") as f:
        f.write(f"🛡️ AUREUM SENTINEL V86\n⏱️ Laufzeit: {int(time.time()-START_TIME)}s\n📦 Neue Assets: {len(new_data)}\n🗺️ Mappings: {len(ticker_map)}")

if __name__ == "__main__": run_v86()
