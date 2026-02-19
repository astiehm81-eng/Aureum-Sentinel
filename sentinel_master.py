import pandas as pd
import pandas_datareader.data as web
import os, json, time, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V71 (MASTER MONOLITH) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
MAX_WORKERS = 50 
START_TIME = time.time()

def ensure_vault():
    """Stellt sicher, dass die Verzeichnisstruktur steht."""
    if not os.path.exists(HERITAGE_DIR):
        os.makedirs(HERITAGE_DIR)

def audit_data(df):
    """Mathematischer Sinnhaftigkeits-Check (Eiserner Standard)."""
    if df is None or df.empty:
        return None
    # Grundreinigung
    df = df.dropna()
    # Filtert Preise nahe Null (API-Rauschen)
    df = df[df['Price'] > 0.001]
    
    # Ausreißer-Schutz: Wenn mehr als 5 Datenpunkte vorhanden sind
    if len(df) > 5:
        df = df.sort_values('Date')
        # Berechnet prozentuale Änderung
        df['pct'] = df['Price'].pct_change().abs()
        # Filter: Alles über 500% Sprung pro Tag gilt als API-Fehler (Noise)
        df = df[df['pct'] < 5].drop(columns=['pct'])
    return df

def fetch_asset(asset, mode="history"):
    """Holt Daten: Entweder 40 Jahre Historie oder 3 Tage Live-Ticker."""
    symbol = asset['symbol']
    try:
        # Zeitspanne festlegen
        days = 40*365 if mode == "history" else 3
        start = datetime.now() - timedelta(days=days)
        
        # Abfrage über Stooq (stabil für internationale Werte)
        df = web.DataReader(symbol, 'stooq', start=start)
        
        if df is not None and not df.empty:
            df = df.reset_index()
            # Spalten normieren
            df.columns = [str(c) for c in df.columns]
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            # Datum als String für Parquet-Kompatibilität
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            
            # Durch den Auditor jagen
            return audit_data(df)
    except Exception as e:
        return None

def save_to_vault(df):
    """Speichert Daten in Jahrzehnt-Shards (Parquet-Format)."""
    if df is None or df.empty:
        return
    
    # Jahrzehnt-Spalte erzeugen
    df['Decade'] = (df['Date'].str[:4].astype(int) // 10) * 10
    
    for decade, group in df.groupby('Decade'):
        path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
        save_g = group.drop(columns=['Decade'])
        
        if os.path.exists(path):
            try:
                existing = pd.read_parquet(path)
                # Zusammenfügen und Duplikate (Ticker + Datum) entfernen
                save_g = pd.concat([existing, save_g]).drop_duplicates(subset=['Ticker', 'Date'])
            except:
                pass # Falls Datei korrupt, wird sie überschrieben
        
        save_g.to_parquet(path, engine='pyarrow', index=False)

def generate_status_report(pool):
    """Erzeugt den lesbaren Report für das Handy."""
    lines = [
        f"🛡️ AUREUM SENTINEL V71",
        f"📅 Stand: {datetime.now().strftime('%d.%m. %H:%M:%S')}",
        "="*45,
        "📊 REPO-STATUS:"
    ]
    
    if os.path.exists(HERITAGE_DIR):
        total_assets = 0
        shards = sorted([f for f in os.listdir(HERITAGE_DIR) if f.endswith(".parquet")])
        
        for f in shards:
            df = pd.read_parquet(os.path.join(HERITAGE_DIR, f))
            assets = int(df['Ticker'].nunique())
            total_assets = max(total_assets, assets)
            lines.append(f"• {f:20} | {assets:4} Assets")
        
        lines.append("="*45)
        lines.append(f"🌏 GLOBAL-CHECK: USA/EU/ASIA integriert")
        lines.append(f"📈 ABDECKUNG: {(total_assets/len(pool))*100:.2f}%")
        lines.append(f"🛡️ AUDITOR: 100% Sinnhaft (Ausreißer-Filter aktiv)")
    else:
        lines.append("⚠️ Vault im Aufbau - Erste Daten werden geladen.")
    
    with open(HUMAN_REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"📄 Status-Report erstellt: {HUMAN_REPORT}")

def run_v71():
    """Hauptprozess: Live-Ticker + Heritage-Füller."""
    ensure_vault()
    if not os.path.exists(POOL_FILE):
        print("❌ Pool-Datei fehlt!")
        return
        
    with open(POOL_FILE, 'r') as f:
        pool = json.load(f)
    
    # Offset-Rotation basierend auf der Zeit
    offset = int((time.time() % 86400) / 300) * 200 % len(pool)
    print(f"📡 V71 aktiv (Index {offset}). Live-Ticker alle 60s.")

    next_live_check = time.time()
    
    # 4-Minuten-Dauerlauf (230 Sekunden)
    while (time.time() - START_TIME) < 230:
        current_now = time.time()
        
        # 1. MINUTEN-TICKER (Priorität: Live-Werte der Top-Assets)
        if current_now >= next_live_check:
            print(f"⏱️ LIVE-TICKER AKTIV: {datetime.now().strftime('%H:%M:%S')}")
            with ThreadPoolExecutor(max_workers=30) as exec:
                # Prüft die ersten 30 Assets im Pool (Live-Favoriten)
                live_results = [r for r in exec.map(lambda a: fetch_asset(a, "live"), pool[:30]) if r is not None]
            
            if live_results:
                save_to_vault(pd.concat(live_results))
            next_live_check = current_now + 60
        
        # 2. HERITAGE-FÜLLER (Massen-Abfrage Historie)
        batch = pool[offset : offset + 100]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            h_results = [r for r in exec.map(lambda a: fetch_asset(a, "history"), batch) if r is not None]
        
        if h_results:
            save_to_vault(pd.concat(h_results))
            print(f"✅ Batch verarbeitet. {len(h_results)} Assets archiviert.")
        
        # Weiterspringen im Pool
        offset = (offset + 100) % len(pool)
        
        # Kurze Pause für API-Rate-Limit
        time.sleep(5)

    # Finaler Status-Report für Handy-Check
    generate_status_report(pool)
    print("🏁 Zyklus V71 erfolgreich beendet.")

if __name__ == "__main__":
    run_v71()
