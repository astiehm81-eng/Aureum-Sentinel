import pandas as pd
import yfinance as yf
import os
import json
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION V151 ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
ANCHOR_FILE = "anchors_memory.json"
AUDIT_FILE = "heritage_audit.txt"

ANCHOR_THRESHOLD = 0.0005 
LOOKBACK_MINUTES = 60      
MAX_WORKERS = 25           
file_lock = threading.Lock()

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        self.known_assets = set()
        self.failed_assets = [] # Liste für fehlerhafte Ticker
        self._load_metadata()

    def _load_metadata(self):
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass

    def process_asset(self, symbol):
        try:
            t = yf.Ticker(symbol)
            # Versuche Preis abzurufen
            df_recent = t.history(period="1d", interval="1m").tail(LOOKBACK_MINUTES)
            
            if df_recent.empty:
                # Zweiter Versuch mit 5d falls Markt gerade erst öffnet
                df_recent = t.history(period="5d", interval="1m").tail(LOOKBACK_MINUTES)
                
            if df_recent.empty:
                log("ERROR", f"❌ Keine Daten für {symbol}. Markiert zur Bereinigung.")
                return {"fail": symbol}

            df_recent = df_recent.reset_index().rename(columns={'Date':'Date','Datetime':'Date','Close':'Price'})
            df_recent['Date'] = pd.to_datetime(df_recent['Date'], utc=True).dt.tz_localize(None)
            df_recent['Ticker'] = symbol
            
            current_price = df_recent['Price'].iloc[-1]
            last_anchor = self.anchors.get(symbol)
            
            # Anker-Logik
            if last_anchor is None or abs(current_price - last_anchor) / last_anchor >= ANCHOR_THRESHOLD:
                self.anchors[symbol] = current_price
                log("TICK", f"⚓ {symbol}: {current_price}")
                # Hier würde das Speichern in Heritage erfolgen (analog zu V150)
            
            return {"Date": df_recent['Date'].iloc[-1], "Ticker": symbol, "Price": current_price}
        except Exception as e:
            log("ERROR", f"⚠️ Kritischer Fehler bei {symbol}: {e}")
            return {"fail": symbol}

    def cleanup_pool(self, symbols_to_remove):
        """Entfernt ungültige Symbole permanent aus der JSON."""
        if not symbols_to_remove: return
        with file_lock:
            with open(POOL_FILE, "r") as f:
                pool = json.load(f)
            
            new_pool = [a for a in pool if a['symbol'] not in symbols_to_remove]
            
            with open(POOL_FILE, "w") as f:
                json.dump(new_pool, f, indent=4)
            log("CLEANUP", f"🧹 {len(symbols_to_remove)} ungültige Assets aus Pool entfernt.")

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        results = []
        to_cleanup = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_asset = {executor.submit(self.process_asset, a['symbol']): a for a in pool}
            for future in as_completed(future_to_asset):
                res = future.result()
                if res:
                    if "fail" in res: to_cleanup.append(res["fail"])
                    else: results.append(res)
        
        # Pool bereinigen
        if to_cleanup:
            self.cleanup_pool(to_cleanup)
            
        if results:
            log("PROGRESS", f"✅ Zyklus beendet. {len(results)} aktiv, {len(to_cleanup)} entfernt.")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
