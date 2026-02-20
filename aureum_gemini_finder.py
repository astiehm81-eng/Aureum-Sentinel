import google.generativeai as genai
import json
import os
import yfinance as yf
import sys
from datetime import datetime

# --- KONFIGURATION ---
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-pro')
POOL_FILE = "isin_pool.json"

def log(msg):
    sys.stdout.write(f"🔍 [FINDER] {msg}\n")
    sys.stdout.flush()

def verify(symbol):
    try:
        return not yf.Ticker(symbol).history(period="1d").empty
    except: return False

def search_massive():
    # Erweiterte Liste für echtes Wachstum
    queries = [
        "S&P 500 Ticker Liste Yahoo", "Nasdaq 100 Ticker Liste Yahoo",
        "DAX Performance Index Ticker", "MDAX Ticker Liste", "SDAX Ticker Liste",
        "CAC 40 Ticker", "AEX Ticker", "IBEX 35 Ticker", "FTSE MIB Ticker"
    ]
    found = []
    for q in queries:
        try:
            log(f"Suche nach: {q}")
            response = model.generate_content(f"Liste Yahoo Ticker für {q}. NUR JSON: [{{'symbol': '...'}}]")
            data = json.loads(response.text.strip().replace("```json", "").replace("```", ""))
            found.extend(data)
        except: continue
    return found

if __name__ == "__main__":
    funde = search_massive()
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, "r") as f: pool = json.load(f)
    else: pool = []
    
    existing = {a['symbol'] for a in pool}
    added = 0
    for f in funde:
        if f['symbol'] not in existing and verify(f['symbol']):
            pool.append(f)
            existing.add(f['symbol'])
            added += 1
            log(f"NEU: {f['symbol']}")

    with open(POOL_FILE, "w") as f:
        json.dump(pool, f, indent=4)
    log(f"Wachstum abgeschlossen: +{added}. Gesamt: {len(pool)}")
