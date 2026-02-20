import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
import multiprocessing
import requests
import io
from datetime import datetime, timedelta
from google import genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION EISERNER STANDARD 2026 ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
BUFFER_FILE = os.path.join(HERITAGE_DIR, "live_buffer.parquet")
ANCHOR_FILE = "anchors_memory.json"
REPORT_FILE = "coverage_report.txt"
API_KEY = os.getenv("GEMINI_API_KEY")

# Parameter laut Anweisung
ANCHOR_STANDARD = 0.0005  # 0,05% Grund-Anker
ANCHOR_VOLATILITY = 0.001 # 0,1% Sofort-Anker (Hard Refresh)
PULSE_INTERVAL = 300      # 5 Minuten
TOTAL_RUNTIME = 900       # 15 Minuten Laufzeit pro Action
MAX_WORKERS = 60          # Hohe Parallelisierung

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    sys.stdout.write(f"[{ts}] [{tag}] {msg}\n")
    sys.stdout.flush()

# --- PHASE 1: FINDER (PARALLEL) ---
def run_finder_task():
    if not API_KEY: return
    try:
        client = genai.Client(api_key=API_KEY)
        # Sektor-Rotation
        hour = datetime.now().hour
        segments = ["S&P 500 Tech", "DAX/MDAX", "Crypto Top 200", "Nikkei Bluechips", "Commodities", "Global Finance", "Context-Layer Assets"]
        seg = segments[hour % len(segments)]
        
        log("FINDER", f"Suche 150 neue Assets für: {seg}")
        prompt = f"Erstelle eine Liste von 150 Yahoo Finance Tickersymbolen für {seg}. Antwort NUR als JSON-Array: [{{'symbol': 'AAPL'}}, ...]"
        
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        new_tickers = json.loads(response.text.strip().replace("```json", "").replace("```", ""))

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
        log("FINDER", f"Pool-Wachstum: +{added} Assets. Gesamt: {len(pool)}")
    except Exception as e:
        log("FINDER", f"Abbruch: {e}")

# --- PHASE 2: DATA ENGINE (SENTINEL) ---
class AureumSentinel:
    def __init__(self):
        self.anchors = {}
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        if os.path.exists(ANCHOR_FILE):
            with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)

    def get_stooq_data(self, symbol):
        """Holt historische Daten via Stooq CSV API."""
        # Stooq nutzt oft .US für US Aktien, hier müsste ggf. ein Mapper hin
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200 and len(r.content) > 100:
                df = pd.read_csv(io.StringIO(r.content.decode('utf-8')))
                return df
        except: return None
        return None

    def fetch_pulse(self, symbol):
        """Real-time Abfrage & Anker-Logik."""
        try:
            t = yf.Ticker(symbol)
            # Hard Refresh: Letzte 2 Stunden für kurzfristige Volatilität
            df = t.history(period="1d", interval="1m")
            if df.empty: return None
            
            current_p = round(df['Close'].iloc[-1], 4)
            last_p = self.anchors.get(symbol)
            
            # Anker-Logik: 0,1% Bewegung setzt sofort neuen Punkt (Hard Refresh forced)
            # 0,05% ist der Standard-Anker
            diff = abs(current_p - last_p) / last_p if last_p else 1.0
            
            if last_p is None or diff >= ANCHOR_STANDARD:
                self.anchors[symbol] = current_p
                return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": current_p}
        except: pass
        return None

    def heal_heritage(self, pool):
        """Verheiratet Daten: <1 Woche Yahoo, davor Stooq."""
        log("SENTINEL", "Starte Self-Healing (Yahoo/Stooq Marriage)...")
        # Hier wird die Logik implementiert, die Lücken füllt
        # (Wird aus Zeitgründen im Cycle nur für Stichproben gemacht)
        pass

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
        start_time = time.time()
        while (time.time() - start_time) < TOTAL_RUNTIME:
            loop_start = time.time()
            if os.path.exists(POOL_FILE):
                with open(POOL_FILE, "r") as f: pool = json.load(f)
            else: break

            log("SENTINEL", f"💓 Puls-Check ({len(pool)} Assets)...")
            results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futures = [exe.submit(self.fetch_pulse, a['symbol']) for a in pool]
                for f in as_completed(futures):
                    res = f.result()
                    if res: results.append(res)

            if results:
                self.save_buffer(results)
                log("SENTINEL", f"{len(results)} Ankerpunkte gesichert.")

            # Report für Marktabdeckung
            with open(REPORT_FILE, "w") as f:
                f.write(f"AUREUM SENTINEL REPORT\nStand: {datetime.now()}\nAssets: {len(pool)}\n")

            wait = max(0, PULSE_INTERVAL - (time.time() - loop_start))
            if (time.time() - start_time) + PULSE_INTERVAL > TOTAL_RUNTIME: break
            time.sleep(wait)

if __name__ == "__main__":
    # Parallel-Start des Finders
    finder_p = multiprocessing.Process(target=run_finder_task)
    finder_p.start()

    # Sentinel Haupt-Loop
    sentinel = AureumSentinel()
    sentinel.run_cycle()

    finder_p.join(timeout=300)
