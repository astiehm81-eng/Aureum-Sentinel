import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
import multiprocessing
import requests
import io
from datetime import datetime
from google import genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
BUFFER_FILE = os.path.join(HERITAGE_DIR, "live_buffer.parquet")
ANCHOR_FILE = "anchors_memory.json"
REPORT_FILE = "coverage_report.txt"
API_KEY = os.getenv("GEMINI_API_KEY")

ANCHOR_STANDARD = 0.0005  # 0,05%
PULSE_INTERVAL = 300       # 5 Minuten
TOTAL_RUNTIME = 900        # 15 Minuten
MAX_WORKERS = 100          # Erhöht für massives Asset-Handling

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    # flush=True sorgt dafür, dass die Logs sofort in GitHub erscheinen
    print(f"[{ts}] [{tag}] {msg}", flush=True)

# --- PHASE 1: MASSIVE FINDER (Hintergrund-Schleife) ---
def run_finder_continuous():
    if not API_KEY:
        log("FINDER", "ERROR: Kein API_KEY. Suche abgebrochen.")
        return

    client = genai.Client(api_key=API_KEY)
    
    # Der Finder läuft so lange, wie der Sentinel läuft
    start_finder = time.time()
    while (time.time() - start_finder) < (TOTAL_RUNTIME - 60):
        try:
            hour = datetime.now().hour
            segments = ["S&P 500", "Nasdaq Tech", "DAX/MDAX/SDAX", "Crypto Top 500", "Commodities", "Global Finance", "Emerging Markets"]
            seg = segments[int(time.time()) % len(segments)]
            
            log("FINDER", f"Mining neue Ticker für Sektor: {seg}...")
            prompt = f"Gib mir 200 unterschiedliche Yahoo Tickersymbole für {seg}. Antwort NUR als JSON-Array: [{{'symbol': 'AAPL'}}, ...]"
            
            response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            raw_text = response.text.strip().replace("```json", "").replace("```", "")
            new_tickers = json.loads(raw_text)

            if os.path.exists(POOL_FILE):
                with open(POOL_FILE, "r") as f: pool = json.load(f)
            else: pool = []

            existing = {a['symbol'] for a in pool}
            added = 0
            for item in new_tickers:
                sym = item['symbol'].upper()
                if sym not in existing:
                    pool.append({"symbol": sym, "added_at": datetime.now().isoformat()})
                    existing.add(sym)
                    added += 1
            
            with open(POOL_FILE, "w") as f:
                json.dump(pool, f, indent=4)
            
            log("FINDER", f"✅ Erfolg: +{added} neue Assets. Pool-Größe jetzt: {len(pool)}")
            
            # Kurze Pause für Gemini Rate-Limits
            time.sleep(30) 
        except Exception as e:
            log("FINDER", f"⚠️ Fehler in Finder-Schleife: {e}")
            time.sleep(10)

# --- PHASE 2: DATA ENGINE (SENTINEL) ---
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
            t = yf.Ticker(symbol)
            df = t.history(period="1d", interval="1m")
            if df.empty: return None
            
            current_p = round(df['Close'].iloc[-1], 4)
            last_p = self.anchors.get(symbol)
            
            # Anker-Logik
            if last_p is None or abs(current_p - last_p) / last_p >= ANCHOR_STANDARD:
                self.anchors[symbol] = current_p
                return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": current_p}
        except: pass
        return None

    def save_buffer(self, results):
        df_new = pd.DataFrame(results)
        if os.path.exists(BUFFER_FILE):
            df_old = pd.read_parquet(BUFFER_FILE)
            df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['Date', 'Ticker'])
        else:
            df_final = df_new
        df_final.to_parquet(BUFFER_FILE, index=False)
        with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)

    def run_cycle(self):
        log("SENTINEL", "System gestartet. Überwachung aktiv.")
        start_time = time.time()
        
        while (time.time() - start_time) < TOTAL_RUNTIME:
            loop_start = time.time()
            
            if os.path.exists(POOL_FILE):
                with open(POOL_FILE, "r") as f: pool = json.load(f)
            else: pool = []

            if not pool:
                log("SENTINEL", "Warte auf Finder (Pool ist noch leer)...")
                time.sleep(10)
                continue

            log("SENTINEL", f"💓 Puls-Check für {len(pool)} Assets...")
            results = []
            
            # Massive Parallelisierung
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futures = {exe.submit(self.fetch_pulse, a['symbol']): a['symbol'] for a in pool}
                for f in as_completed(futures):
                    res = f.result()
                    if res: results.append(res)

            if results:
                self.save_buffer(results)
                log("SENTINEL", f"💾 {len(results)} Ankerpunkte in Parquet gesichert.")
            else:
                log("SENTINEL", "Keine signifikanten Preisbewegungen (>0,05%) erkannt.")

            # Report aktualisieren
            with open(REPORT_FILE, "w") as f:
                f.write(f"AUREUM SENTINEL STATUS\nAbdeckung: {len(pool)} Assets\nLetzter Puls: {datetime.now()}\nStatus: Running\n")

            wait = max(10, PULSE_INTERVAL - (time.time() - loop_start))
            if (time.time() - start_time) + PULSE_INTERVAL > TOTAL_RUNTIME:
                log("SENTINEL", "🏁 Ende des Zyklus erreicht. Bereite Sync vor.")
                break
            
            log("SENTINEL", f"💤 Puls beendet. Warte {int(wait)}s.")
            time.sleep(wait)

if __name__ == "__main__":
    # Finder als Hintergrund-Prozess starten
    finder_p = multiprocessing.Process(target=run_finder_continuous)
    finder_p.start()

    # Haupt-Sentinel
    try:
        sentinel = AureumSentinel()
        sentinel.run_cycle()
    except KeyboardInterrupt:
        log("SYSTEM", "Manueller Abbruch.")
    
    # Finder beenden
    finder_p.terminate()
    log("SYSTEM", "Alle Prozesse beendet.")
