import google.generativeai as genai
import json
import os
import yfinance as yf
from datetime import datetime

# --- KONFIGURATION (V108.7) ---
genai.configure(api_key="DEIN_GEMINI_API_KEY")
model = genai.GenerativeModel('gemini-pro')

POOL_FILE = "isin_pool.json"

def update_status(msg):
    print(f"🔍 [FINDER] {msg}", flush=True)

def verify_ticker(symbol):
    """Kurzcheck, ob der Ticker bei Yahoo Daten liefert."""
    try:
        t = yf.Ticker(symbol)
        return not t.history(period="1d").empty
    except: return False

def search_massive():
    """Systematische Suche für massives Pool-Wachstum."""
    queries = [
        "S&P 500 Ticker Liste Yahoo",
        "Nasdaq 100 Ticker Liste Yahoo",
        "DAX, MDAX, SDAX Ticker Liste Yahoo",
        "EuroStoxx 50 Ticker Liste Yahoo",
        "FTSE 100 Ticker Liste Yahoo"
    ]
    
    found_symbols = []
    for q in queries:
        prompt = f"Liste mir alle Symbole für {q} auf. Antwort NUR als JSON-Array: [{{'symbol': '...'}}, ...]"
        try:
            response = model.generate_content(prompt)
            data = json.loads(response.text.strip().replace("```json", "").replace("```", ""))
            found_symbols.extend(data)
        except: continue
    return found_symbols

def update_pool(new_assets):
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, "r") as f: pool = json.load(f)
    else: pool = []
    
    existing = {a['symbol'] for a in pool}
    added = 0
    
    for a in new_assets:
        if a['symbol'] not in existing:
            if verify_ticker(a['symbol']):
                pool.append(a)
                existing.add(a['symbol'])
                added += 1
                print(f"  + NEU ENTDECKT: {a['symbol']}", flush=True)
                
    with open(POOL_FILE, "w") as f:
        json.dump(pool, f, indent=4)
    update_status(f"Pool wächst: +{added} neue Assets. Gesamtstand: {len(pool)}")

if __name__ == "__main__":
    print("--- FINDER AGENT START ---")
    funde = search_massive()
    update_pool(funde)
