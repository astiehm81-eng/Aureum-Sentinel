import pandas as pd
import yfinance as yf
import os, json, time, threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (V107.8 - DER EISERNE STANDARD: UNZERSTÖRBAR) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
ANCHOR_FILE = "anchors_memory.json"
STATUS_FILE = "vault_status.txt"

ANCHOR_THRESHOLD = 0.0005  # 0,05%
MAX_WORKERS_LIVE = 100     # Massive Parallelität für Yahoo
MAX_WORKERS_HEAL = 20      # Parallelität für Stooq/Self-Healing
REFRESH_RATE = 300         # 5-Minuten-Puls

# Globaler Lock für Datenintegrität (Verhindert Datenbank-Zerstörung)
db_lock = threading.Lock()
anchors = {}

def update_status(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(STATUS_FILE, "a") as f: f.write(f"[{timestamp}] {msg}\n")
    print(f"🛡️ {msg}", flush=True)

# --- SINNHAFTIGKEITSPRÜFUNG (Anti-Blödsinn-Filter) ---
def is_plausible(price, last_price):
    if price <= 0 or pd.isna(price): return False
    if last_price:
        change = abs(price - last_price) / last_price
        if change > 0.15: # Blockiert Yahoo-Glitches > 15%
            return False
    return True

# --- SELF-HEALING & DATABASE PROTECTOR ---
def sync_to_vault(df_new, vault_name):
    """Verheiratet neue Daten mit der Datenbank ohne Duplikate oder Korruption."""
    if df_new.empty: return
    
    with db_lock:
        path = os.path.join(HERITAGE_DIR, f"{vault_name}.parquet")
        if os.path.exists(path):
            try:
                df_old = pd.read_parquet(path)
                # Verheiratung: Alt + Neu, Duplikate raus (Echter Zeitstempel-Check)
                df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['Date', 'Ticker'], keep='last')
            except Exception as e:
                update_status(f"WARNUNG: Datenbank-Reparatur für {vault_name} eingeleitet: {e}")
                df_final = df_new
        else:
            df_final = df_new
            
        df_final.sort_values(['Ticker', 'Date']).to_parquet(path, index=False)

# --- ENGINE A: LIVE PULSE (YAHOO) ---
def live_engine(pool):
    update_status("Engine A (Live Yahoo) gestartet.")
    def process_live(asset):
        symbol = asset['symbol']
        try:
            t = yf.Ticker(symbol)
            # Hard Refresh Logik
            df = t.history(period="1d", interval="1m")
            if not df.empty:
                curr_p = round(df['Close'].iloc[-1], 4)
                last_p = anchors.get(symbol)
                
                if is_plausible(curr_p, last_p):
                    if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                        anchors[symbol] = curr_p
                        return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": curr_p}
        except: pass
        return None

    while True:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_LIVE) as executor:
            results = list(executor.map(process_live, pool))
        
        valid = [r for r in results if r is not None]
        if valid:
            sync_to_vault(pd.DataFrame(valid), "live_buffer")
            with open(ANCHOR_FILE, "w") as f: json.dump(anchors, f)
        
        time.sleep(REFRESH_RATE)

# --- ENGINE B: SELF-HEALING & STOOQ (HERITAGE) ---
def heritage_healer(pool):
    update_status("Engine B (Heritage Healer) prüft Lücken.")
    def heal_asset(asset):
        symbol = asset['symbol']
        try:
            # Stooq-Verheiratung für historische Daten
            url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
            df_stooq = pd.read_csv(url)
            if not df_stooq.empty:
                df_stooq['Ticker'] = symbol
                df_stooq['Date'] = pd.to_datetime(df_stooq['Date'])
                sync_to_vault(df_stooq[['Date', 'Ticker', 'Close']], "historical_vault")
        except: pass

    # Läuft massiv parallel im Hintergrund
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_HEAL) as executor:
        executor.map(heal_asset, pool)
    update_status("Heritage-Heilung abgeschlossen.")

if __name__ == "__main__":
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    if os.path.exists(ANCHOR_FILE):
        with open(ANCHOR_FILE, "r") as f: anchors = json.load(f)
    
    with open(POOL_FILE, "r") as f: pool = json.load(f)

    # PARALLELE WELTEN STARTEN
    # Thread 1: Der Heiler (Stooq)
    healing_thread = threading.Thread(target=heritage_healer, args=(pool,))
    healing_thread.daemon = True
    healing_thread.start()

    # Thread 2: Der Live-Puls (Yahoo) - Hauptprozess
    live_engine(pool)
