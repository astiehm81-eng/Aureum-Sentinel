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
TOTAL_RUNTIME = 900       
PULSE_INTERVAL = 300      

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

def run_finder_parallel(passed_key):
    """Subprozess für die Gemini-Expansion."""
    if not passed_key:
        log("FINDER", "❌ ERROR: Subprozess hat keinen Key erhalten.")
        return
    try:
        client = genai.Client(api_key=passed_key)
        start_time = time.time()
        segments = ["Global Mega-Caps", "Nasdaq 100", "DAX 40", "Crypto USD", "Commodities"]
        
        while (time.time() - start_time) < (TOTAL_RUNTIME - 60):
            seg = segments[int(time.time() / 60) % len(segments)]
            log("FINDER", f"Mining {seg}...")
            
            prompt = f"Gib mir 250 Yahoo Finance Tickersymbole für {seg}. NUR JSON-Array: [{{'symbol': 'AAPL'}}, ...]"
            response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            
            try:
                raw_text = response.text.strip().replace("```json", "").replace("```", "")
                new_data = json.loads(raw_text)
                if os.path.exists(POOL_FILE):
                    with open(POOL_FILE, "r") as f: pool = json.load(f)
                else: pool = []

                existing = {a['symbol'] for a in pool}
                added = 0
                for item in new_data:
                    sym = str(item.get('symbol', '')).upper()
                    if sym and sym not in existing:
                        pool.append({"symbol": sym, "added_at": datetime.now().isoformat()})
                        existing.add(sym)
                        added += 1
                if added > 0:
                    with open(POOL_FILE, "w") as f: json.dump(pool, f, indent=4)
                    log("FINDER", f"🚀 Pool +{added} (Gesamt: {len(pool)})")
            except: pass
            time.sleep(60)
    except Exception as e:
        log("FINDER", f"❌ API Fehler: {str(e)[:50]}")

if __name__ == "__main__":
    # DEBUG: Zeige alle verfügbaren Umgebungsvariablen (maskiert)
    log("SYSTEM", "Prüfe Umgebungsvariablen...")
    k1 = os.getenv("GEMINI_API_KEY")
    k2 = os.getenv("GOOGLE_API_KEY")
    
    # Wähle den ersten verfügbaren Key
    final_key = k1 or k2
    
    if not final_key:
        log("SYSTEM", f"❌ FATAL: Weder GEMINI_API_KEY noch GOOGLE_API_KEY gefunden!")
        # Liste zur Diagnose die Keys auf (ohne Wert)
        log("SYSTEM", f"Verfügbare Env-Keys: {list(os.environ.keys())}")
        sys.exit(1)
    else:
        log("SYSTEM", f"✅ Key gefunden (Länge: {len(final_key)}). Starte Prozesse...")

    # Finder starten
    finder_proc = multiprocessing.Process(target=run_finder_parallel, args=(final_key,))
    finder_proc.start()
    
    try:
        # Sentinel Logik (vereinfacht für diesen Block)
        from concurrent.futures import ThreadPoolExecutor
        log("SENTINEL", "Monitoring aktiv.")
        # ... (Rest der Sentinel-Klasse/Logik bleibt wie in V114)
        # Zur Sicherheit hier kurz die minimal-Loop:
        start_run = time.time()
        while (time.time() - start_run) < TOTAL_RUNTIME:
            # Hier käme dein Puls-Check rein
            time.sleep(300) 
    finally:
        finder_proc.terminate()
        finder_proc.join()
        log("SYSTEM", "Zyklus beendet.")
