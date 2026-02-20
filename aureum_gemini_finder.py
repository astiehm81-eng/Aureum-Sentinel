import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
import multiprocessing
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

ANCHOR_THRESHOLD = 0.0005  # 0,05%
PULSE_INTERVAL = 300       # 5 Minuten
TOTAL_RUNTIME = 900        # 15 Minuten

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    sys.stdout.write(f"[{ts}] [{tag}] {msg}\n")
    sys.stdout.flush()

# --- FINDER LOGIK (Läuft parallel) ---
def run_finder_task():
    if not API_KEY:
        log("FINDER", "ERROR: Kein API_KEY gefunden.")
        return

    try:
        client = genai.Client(api_key=API_KEY)
        segments = ["S&P 500", "DAX", "Crypto", "Nikkei", "Commodities", "Banks"]
        seg = segments[datetime.now().hour % len(segments)]
        
        log("FINDER", f"Suche neue Assets für Sektor: {seg}...")
        prompt = f"Gib mir 100 Yahoo Ticker für {seg} als JSON Array: [{{'symbol': 'AAPL'}}, ...]"
        
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        text = response.text.strip()
        if "```json" in text: text = text.split("```json")[1].split("```")[0]
        new_tickers = json.loads(text)

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
        log("FINDER", f"Pool erweitert um {added} Assets.")
    except Exception as e:
        log("FINDER", f"Fehler: {e}")

# --- SENTINEL LOGIK ---
class AureumSentinel:
    def __init__(self):
        self.anchors = {}
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        if os.path.exists(ANCHOR_FILE):
            with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)

    def fetch_price(self, symbol):
        try:
            t = yf.Ticker(symbol)
            df = t.history(period="1d", interval="1m")
            if df.empty: return None
            p = round(df['Close'].iloc[-1], 4)
            
            last = self.anchors.get(symbol)
            if last is None or abs(p - last)/last >= ANCHOR_THRESHOLD:
                self.anchors[symbol] = p
                return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": p}
        except: pass
        return None

    def save_data(self, results):
        df_new = pd.DataFrame(results)
        if os.path.exists(BUFFER_FILE):
            df_old = pd.read_parquet(BUFFER_FILE)
            df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['Date', 'Ticker'])
        else:
            df_final = df_new
        df_final.to_parquet(BUFFER_FILE, index=False)
        with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)

    def generate_report(self, pool_size):
        with open(REPORT_FILE, "w") as f:
            f.write(f"AUREUM SENTINEL STATUS - {datetime.now()}\n")
            f.write(f"Assets im Pool: {pool_size}\n")
            f.write(f"System-Status: Aktiv / Parallel-Mode\n")

    def run_pulse_cycle(self):
        start_time = time.time()
        while (time.time() - start_time) < TOTAL_RUNTIME:
            loop_start = time.time()
            
            if os.path.exists(POOL_FILE):
                with open(POOL_FILE, "r") as f: pool = json.load(f)
            else: pool = []

            if not pool:
                log("SENTINEL", "Warte auf Finder (Pool leer)...")
                time.sleep(30)
                continue

            log("SENTINEL", f"Puls-Check für {len(pool)} Assets...")
            results = []
            with ThreadPoolExecutor(max_workers=50) as exe:
                futures = [exe.submit(self.fetch_price, a['symbol']) for a in pool]
                for f in as_completed(futures):
                    res = f.result()
                    if res: results.append(res)
            
            if results:
                self.save_data(results)
                log("SENTINEL", f"{len(results)} neue Anker gesichert.")

            self.generate_report(len(pool))
            
            elapsed = time.time() - loop_start
            wait = max(0, PULSE_INTERVAL - elapsed)
            if (time.time() - start_time) + PULSE_INTERVAL > TOTAL_RUNTIME: break
            log("SENTINEL", f"Warte {int(wait)}s bis zum nächsten Puls.")
            time.sleep(wait)

if __name__ == "__main__":
    # Finder in einem eigenen Prozess starten
    finder_proc = multiprocessing.Process(target=run_finder_task)
    finder_proc.start()

    # Sentinel im Hauptprozess ausführen
    sentinel = AureumSentinel()
    sentinel.run_pulse_cycle()

    # Sicherstellen, dass der Finder fertig ist
    finder_proc.join(timeout=300)
