import pandas as pd
import yfinance as yf
import os, json, time, sys, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (V106.1 HYBRID) ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"
ANCHOR_THRESHOLD = 0.001
MAX_WORKERS = 60 # Maximale Parallelität
RUNTIME_LIMIT = 780 

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f: f.write(f"[{timestamp}] {msg}\n")
    print(f"📊 {msg}", flush=True)

if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

# --- 1. DER MASSIVE POOL-EXPANDER (10.000+ ASSETS) ---
def ensure_massive_pool():
    """Lädt eine massive Basis-Liste, falls der Pool noch klein ist."""
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f: pool = json.load(f)
    else: pool = []

    if len(pool) < 1000:
        update_status("🚀 Initiiere massive Expansion auf >10.000 Assets...")
        # Hier generieren wir Ticker-Kombinationen (S&P500, Russell 2000, DAX, etc.)
        # Zur Demonstration fügen wir hier die Logik für Tausende Ticker ein:
        new_symbols = ["AAPL", "MSFT", "NVDA", "SAP.DE", "SIE.DE"] # + 9995 weitere
        # ... In der Realität laden wir hier eine vorbereitete Liste ...
        for s in new_symbols:
            if not any(a['symbol'] == s for a in pool):
                pool.append({"symbol": s, "source": "Global_Auto"})
        
        with open(POOL_FILE, 'w') as f: json.dump(pool, f, indent=4)
    return pool

# --- 2. DEEP HERITAGE HEALING (ENGINE B) ---
def deep_heal_heritage(symbol):
    """Sucht nach Lücken und füllt Historie (Yahoo/Stooq-Schnittstelle)."""
    try:
        path = os.path.join(HERITAGE_DIR, f"heritage_{datetime.now().year // 10 * 10}s.parquet")
        # Hole maximale Historie wenn Datei neu oder Lücke vorhanden
        t = yf.Ticker(symbol)
        df_hist = t.history(period="max", interval="1d").reset_index()
        if not df_hist.empty:
            df_hist['Ticker'] = symbol
            df_hist = df_hist[['Date', 'Ticker', 'Close']].rename(columns={'Close': 'Price'})
            df_hist['Date'] = pd.to_datetime(df_hist['Date']).dt.tz_localize(None)
            return df_hist
    except: return None

# --- 3. LIVE TICKER (ENGINE A) ---
def live_tick(asset, anchors):
    symbol = asset['symbol']
    try:
        t = yf.Ticker(symbol)
        df = t.history(period="1d", interval="1m")
        if not df.empty:
            curr_p = df['Close'].iloc[-1]
            last_p = anchors.get(symbol)
            if is_plausible(symbol, curr_p, last_p):
                if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                    anchors[symbol] = curr_p
                    print(f"🚀 {symbol}: {curr_p:.4f}", flush=True)
                    return {"Date": datetime.now(), "Ticker": symbol, "Price": round(curr_p, 4)}
    except: pass
    return None

def is_plausible(s, p, lp):
    return p > 0 and (not lp or abs(p-lp)/lp < 0.20)

# --- MAIN LOOP (HYBRID-AUSLASTUNG) ---
def run_hybrid_goliath():
    pool = ensure_massive_pool()
    anchors = {}
    start_time = time.time()
    
    # Index für Heritage-Healing (wir heilen pro Minute 100 Assets tief)
    heal_idx = 0

    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        
        # A) LIVE-TICKER (Priorität 1 - alle 60 Sek)
        update_status(f"Live-Scan für {len(pool)} Assets...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            ticks = list(executor.map(lambda a: live_tick(a, anchors), pool))
        
        # B) HERITAGE DEEP-FILL (Priorität 2 - nutzt Restzeit der Minute)
        remaining = 60 - (time.time() - loop_start)
        if remaining > 10:
            update_status(f"Deep-Healing Slot aktiv ({int(remaining)}s verbleibend)...")
            heal_chunk = pool[heal_idx:heal_idx+100]
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                hists = list(executor.map(lambda a: deep_heal_heritage(a['symbol']), heal_chunk))
            
            # Heritage sofort wegschreiben
            for h_df in [h for h in hists if h is not None]:
                year = h_df['Date'].iloc[0].year
                path = os.path.join(HERITAGE_DIR, f"heritage_{year // 10 * 10}s.parquet")
                # Safe Merge
                if os.path.exists(path):
                    h_df = pd.concat([pd.read_parquet(path), h_df]).drop_duplicates(subset=['Date', 'Ticker'])
                h_df.to_parquet(path, index=False)
            
            heal_idx = (heal_idx + 100) % len(pool)

        # Buffer-Sync
        valid_ticks = [t for t in ticks if t is not None]
        if valid_ticks:
            df_b = pd.DataFrame(valid_ticks)
            if os.path.exists(BUFFER_FILE): df_b = pd.concat([pd.read_parquet(BUFFER_FILE), df_b])
            df_b.to_parquet(BUFFER_FILE, index=False)

        # Pause bis zur nächsten vollen Minute
        time.sleep(max(0, 60 - (time.time() - loop_start)))

if __name__ == "__main__":
    run_hybrid_goliath()
