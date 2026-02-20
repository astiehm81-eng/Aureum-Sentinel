import pandas as pd
import yfinance as yf
import os, json, time, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V80 (GAP-AUDIT & SMART-FILL) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
GAP_FILE = "missing_assets.json" # Hier landen die ISINs für den nächsten Lauf
MAX_WORKERS = 35 
START_TIME = time.time()

def run_gap_audit(pool):
    """Prüft den Bestand und schreibt eine Liste der fehlenden/unvollständigen Assets."""
    print("🔍 Starte Gap-Audit...")
    existing_stats = {}
    
    if os.path.exists(HERITAGE_DIR):
        for f in glob.glob(f"{HERITAGE_DIR}/*.parquet"):
            try:
                # Wir laden nur Ticker und Datum für den Speed-Check
                df = pd.read_parquet(f, columns=['Ticker', 'Date'])
                for ticker, group in df.groupby('Ticker'):
                    count = len(group)
                    existing_stats[ticker] = existing_stats.get(ticker, 0) + count
            except: continue

    missing = []
    incomplete = []
    
    for asset in pool:
        symbol = asset['symbol']
        if symbol not in existing_stats:
            missing.append(asset)
        elif existing_stats[symbol] < 500: # Weniger als ca. 2 Jahre Daten
            incomplete.append(asset)
            
    # Speichere die "Fahndungsliste"
    with open(GAP_FILE, 'w') as f:
        json.dump({"missing": missing, "incomplete": incomplete}, f, indent=4)
    
    return missing, incomplete, existing_stats

def fetch_asset(asset):
    symbol = asset['symbol']
    try:
        # Yahoo Finance für stabile Historie
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="max") # Hole alles Verfügbare
        
        if df is not None and not df.empty:
            df = df.reset_index()
            # Spalten-Normierung
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df['Ticker'] = symbol
            
            # Auditor: Filtert krasse Sprünge (Noise)
            if len(df) > 1:
                df = df.sort_values('Date')
                df['pct'] = df['Price'].pct_change().abs()
                df = df[(df['pct'] < 0.6) | (df['pct'].isna())].drop(columns=['pct'])
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

def run_v80():
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    # 1. PARALLELES AUDIT
    missing, incomplete, stats = run_gap_audit(pool)
    
    # 2. PRIORISIERUNG: Erst die fehlenden, dann die unvollständigen
    target_list = missing + incomplete
    print(f"🎯 Ziel: {len(missing)} fehlende und {len(incomplete)} unvollständige Assets.")

    # 3. SMART-FILL (150 Sekunden Laufzeit)
    offset = int((time.time() % 86400) / 300) * 50 % max(1, len(target_list))
    
    while (time.time() - START_TIME) < 150:
        batch = target_list[offset : offset + 30]
        if not batch: break
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = [r for r in ex.map(fetch_asset, batch) if r is not None]
        
        if results:
            save_to_shards(pd.concat(results))
            print(f"📥 {len(results)} Assets erfolgreich nachgeladen.")
        
        offset += 30
        if offset >= len(target_list): break
        time.sleep(2)

    # 4. FINALER REPORT
    final_missing, final_incomplete, _ = run_gap_audit(pool)
    with open(HUMAN_REPORT, "w", encoding="utf-8") as f:
        report = [
            f"🛡️ AUREUM SENTINEL V80 - STATUS",
            f"📅 {datetime.now().strftime('%H:%M:%S')}",
            "="*35,
            f"✅ Assets im Vault: {len(pool) - len(final_missing)}",
            f"❌ Total Fehlend:   {len(final_missing)}",
            f"⚠️ Unvollständig:   {len(final_incomplete)}",
            f"📊 Abdeckung:       {((len(pool)-len(final_missing))/len(pool))*100:.2f}%",
            "="*35,
            "🔎 TOP FEHLEND (Fahndungsliste):",
            *[f"• {a['symbol']}" for a in final_missing[:15]]
        ]
        f.write("\n".join(report))

if __name__ == "__main__":
    run_v80()
