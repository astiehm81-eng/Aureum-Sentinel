import pandas as pd
import yfinance as yf
import os
import json
import threading
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION V154 ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
ANCHOR_FILE = "anchors_memory.json"
DISCOVERY_FILE = "discovery_candidates.json"

ANCHOR_THRESHOLD = 0.0005 
LOOKBACK_MINUTES = 60      
MAX_WORKERS = 50         
RATE_LIMIT_HIT = False

# --- HILFSFUNKTIONEN ---
def log(tag, msg):
    """Zentrales Logging für den Aureum Sentinel."""
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        self._load_anchors()

    def _load_anchors(self):
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass

    def discover_peers(self, symbol):
        """Sucht nach verwandten Assets (Peers) eines Tickers."""
        try:
            t = yf.Ticker(symbol)
            # Nutze die Yahoo-Empfehlungen für ähnliche Unternehmen
            peers = t.info.get('recommendationKey', []) 
            # Falls Yahoo direkt Peers liefert (oft unter 'related' oder 'peers')
            # Hier simulieren wir die Suche über den Sektor
            sector = t.info.get('sector')
            if sector:
                log("DISCOVERY", f"Peer-Analyse für Sektor '{sector}' gestartet (via {symbol}).")
                # Wir merken uns den Fund für die Kandidatenliste
                return {"sector": sector, "origin": symbol}
        except:
            return None

    def process_asset(self, symbol):
        global RATE_LIMIT_HIT
        if RATE_LIMIT_HIT: return None
        try:
            time.sleep(random.uniform(0.1, 0.3))
            t = yf.Ticker(symbol)
            df = t.history(period="1d", interval="1m").tail(LOOKBACK_MINUTES)
            
            if df.empty: return {"fail": symbol}

            df = df.reset_index().rename(columns={'Date':'Date','Datetime':'Date','Close':'Price'})
            df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
            current_price = df['Price'].iloc[-1]
            
            # Anker-Check
            last_anchor = self.anchors.get(symbol)
            if last_anchor is None or abs(current_price - last_anchor) / last_anchor >= ANCHOR_THRESHOLD:
                self.anchors[symbol] = current_price
                log("TICK", f"⚓ {symbol}: {current_price}")
            
            return {"Date": df['Date'].iloc[-1], "Ticker": symbol, "Price": current_price}
        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                RATE_LIMIT_HIT = True
            return None

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        symbols = [a['symbol'] for a in pool]
        results = []
        
        log("SYSTEM", f"Puls-Check für {len(symbols)} Assets...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_asset = {executor.submit(self.process_asset, s): s for s in symbols}
            for future in as_completed(future_to_asset):
                res = future.result()
                if res and "Price" in res: results.append(res)

        # --- DISCOVERY LOGIK (Wird bei 5% der Läufe getriggert) ---
        if random.random() < 0.05 and results:
            lucky_pick = random.choice(results)['Ticker']
            discovery_res = self.discover_peers(lucky_pick)
            if discovery_res:
                # Speichern der Discovery-Info (kann später für automatische Adds genutzt werden)
                with open(DISCOVERY_FILE, "a") as f:
                    f.write(json.dumps({str(datetime.now()): discovery_res}) + "\n")

        # Speichern & Abschluss
        with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
        log("PROGRESS", f"✅ Zyklus beendet. {len(results)} Ticks verarbeitet.")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
