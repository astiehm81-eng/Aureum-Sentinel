import google.generativeai as genai
import json
import os
import yfinance as yf
import sys

# --- KONFIGURATION V109 ---
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-pro')
POOL_FILE = "isin_pool.json"

def log(msg):
    sys.stdout.write(f"🔍 [FINDER] {msg}\n")
    sys.stdout.flush()

def verify(symbol):
    try: return not yf.Ticker(symbol).history(period="1d").empty
    except: return False

def search_massive():
    # Erweiterte Queries für 10.000+ Assets
    queries = [
        "S&P 500 Ticker Liste Yahoo", "Nasdaq 100 Ticker Liste Yahoo",
        "DAX, MDAX, SDAX Ticker Yahoo", "Stoxx Europe 600 Ticker Yahoo",
        "Nikkei 225 Ticker Yahoo", "Top 300 Cryptocurrencies Yahoo Symbols"
    ]
    found = []
    for q in queries:
        try:
            log(f"Gemini-Suche: {q}")
            response = model.generate_content(f"Liste Yahoo Finance Symbole für {q}. Antwort NUR als JSON-Array: [{{'symbol': '...'}}]")
            data = json.loads(response.text.strip().replace("```json", "").replace("```", ""))
            found.extend(data)
        except: continue
    return found

if __name__ == "__main__":
    new_found = search_massive()
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, "r") as f: pool = json.load(f)
    else: pool = []
    
    existing = {a['symbol'] for a in pool}
    added = 0
    for f in new_found:
        if f['symbol'] not in existing and verify(f['symbol']):
            pool.append(f)
            existing.add(f['symbol'])
            added += 1
            log(f"NEUES ASSET ENTDECKT: {f['symbol']}")

    with open(POOL_FILE, "w") as f:
        json.dump(pool, f, indent=4)
    log(f"Wachstums-Zyklus beendet: +{added} Assets. Gesamt-Pool: {len(pool)}")
