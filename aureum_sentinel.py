import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
import multiprocessing
import requests
from datetime import datetime
from google import genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- ROBUSTE KEY-ABFRAGE ---
API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")

POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
BUFFER_FILE = os.path.join(HERITAGE_DIR, "live_buffer.parquet")
ANCHOR_FILE = "anchors_memory.json"
REPORT_FILE = "coverage_report.txt"

ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      
TOTAL_RUNTIME = 900       
MAX_WORKERS = 100         

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

def run_finder_continuous():
    if not API_KEY:
        log("FINDER", "❌ ERROR: API_KEY ist leer! Prüfe GitHub Secrets 'GEMINI_API_KEY'.")
        return

    try:
        client = genai.Client(api_key=API_KEY)
        start_finder = time.time()
        
        while (time.time() - start_finder) < (TOTAL_RUNTIME - 60):
            # Sektoren-Rotation basierend auf aktueller Sekunde für Varianz
            segments = ["Global Mega-Caps", "Nasdaq 100", "S&P 500", "DAX 40", "Crypto USD", "Commodities"]
            seg = segments[int(time.time()) % len(segments)]
            
            log("FINDER", f"Mining Ticker für: {seg}...")
            prompt = f"Gib mir 250 Yahoo Finance Tickersymbole für {seg}. NUR JSON-Array: [{{'symbol': 'AAPL'}}, ...]"
            
            # Modell auf gemini-2.0-flash setzen für Speed
            response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            raw_text = response.text.strip().replace("```json", "").replace("```", "")
            new_tickers = json.loads(raw_text)

            if os.path.exists(POOL_FILE):
                with open(POOL_FILE, "r") as f: pool = json.load(f)
            else: pool = []

            existing = {a['symbol'] for a in pool}
            added = 0
            for item in new_tickers:
                sym = str(item['symbol']).upper()
                if sym not in existing:
                    pool.append({"symbol": sym, "added_at": datetime.now().isoformat()})
                    existing.add(sym)
                    added += 1
            
            if added > 0:
                with open(POOL_FILE, "w") as f:
                    json.dump(pool, f, indent=4)
                log("FINDER", f"🚀 Pool-Update: +{added} Ticker (Gesamt: {len(pool)})")
            
            time.sleep(15) # Schnellerer Turnus
    except Exception as e:
        log("FINDER", f"⚠️ Fehler: {str(e)[:100]}")
        time.sleep(10)

class AureumSentinel:
    def __init__(self):
        self.anchors = {}
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: self.anchors = {}

    def fetch_pulse(self, symbol):
        try:
            # Schneller Check ohne History-Overhead für den Live-Puls
            t = yf.Ticker(symbol)
            data = t.basic_info
            current_p = data.get('lastPrice') or data.get('previousClose')
            
            if not current_p:
                df = t.history(period="1d")
                if not df.empty: current_p = df['Close'].iloc[-1]
            
            if not current_p: return None
            
            current_p = round(float(current_p), 4)
            last_p = self.anchors.get(symbol)
            
            if last_p is None or abs(current_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                self.anchors[symbol] = current_p
                return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": current_p}
        except: pass
        return None

    def run_cycle(self):
        log("SENTINEL", "Aureum Sentinel aktiv.")
        start_time = time.time()
        
        while (time.time() - start_time) < TOTAL_RUNTIME:
            loop_start = time.time()
            if os.path.exists(POOL_FILE):
                with open(POOL_FILE, "r") as f: pool = json.load(f)
            else: pool = []

            if not pool:
                log("SENTINEL", "Warten auf Pool-Initialisierung...")
                time.sleep(10)
                continue

            log("SENTINEL", f"💓 Puls-Check ({len(pool)} Assets)...")
            results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futures = {exe.submit(self.fetch_pulse, a['symbol']): a['symbol'] for a in pool}
                for f in as_completed(futures):
                    res = f.result()
                    if res: results.append(res)

            if results:
                df_new = pd.DataFrame(results)
                if os.path.exists(BUFFER_FILE):
                    df_old = pd.read_parquet(BUFFER_FILE)
                    df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['Date', 'Ticker'])
                else:
                    df_final = df_new
                df_final.to_parquet(BUFFER_FILE, index=False)
                with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
                log("SENTINEL", f"💾 {len(results)} Anker gesichert.")

            with open(REPORT_FILE, "w") as f:
                f.write(f"AUREUM SENTINEL\nAbdeckung: {len(pool)}\nStand: {datetime.now()}\n")

            wait = max(10, PULSE_INTERVAL - (time.time() - loop_start))
            if (time.time() - start_time) + PULSE_INTERVAL > TOTAL_RUNTIME: break
            time.sleep(wait)

if __name__ == "__main__":
    finder_p = multiprocessing.Process(target=run_finder_continuous)
    finder_p.start()
    try:
        AureumSentinel().run_cycle()
    finally:
        finder_p.terminate()
        log("SYSTEM", "Zyklus beendet.")
