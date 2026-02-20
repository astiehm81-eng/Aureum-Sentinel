import google.generativeai as genai
import json
import os
import yfinance as yf
from datetime import datetime

# --- KONFIGURATION (V108.5 FINDER-AGENT) ---
genai.configure(api_key="DEIN_GEMINI_API_KEY")
model = genai.GenerativeModel('gemini-pro')

POOL_FILE = "isin_pool.json"
STATUS_FILE = "vault_status.txt"

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f:
        f.write(f"[{timestamp}] [FINDER] {msg}\n")
    print(f"🔍 {msg}", flush=True)

def verify_ticker(symbol):
    try:
        t = yf.Ticker(symbol)
        info = t.history(period="1d")
        return not info.empty
    except:
        return False

def search_tickers_with_gemini(sector_query):
    prompt = f"""
    Agiere als Finanzdaten-Spezialist. Erstelle eine Liste von Yahoo Finance Tickern 
    für: {sector_query}.
    Die Ticker müssen das Format für Deutschland (z.B. SAP.DE) oder USA (z.B. AAPL) haben.
    Antworte NUR im JSON-Format: [{"symbol": "TICKER"}, ...]
    """
    try:
        response = model.generate_content(prompt)
        raw_text = response.text.strip()
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[1].split("```")[0]
        return json.loads(raw_text)
    except Exception as e:
        update_status(f"Gemini-Fehler: {e}")
        return []

def update_isin_pool(new_assets):
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, "r") as f:
            pool = json.load(f)
    else:
        pool = []
    
    existing_symbols = {a['symbol'] for a in pool}
    added_count = 0
    for asset in new_assets:
        sym = asset['symbol']
        if sym not in existing_symbols:
            if verify_ticker(sym):
                pool.append(asset)
                existing_symbols.add(sym)
                added_count += 1
    with open(POOL_FILE, "w") as f:
        json.dump(pool, f, indent=4)
    update_status(f"Pool-Update: +{added_count} Assets. Gesamt: {len(pool)}")

if __name__ == "__main__":
    anfrage = "Top 100 Mid-Cap Unternehmen im MDAX und SDAX"
    update_status(f"Suche läuft für: {anfrage}")
    funde = search_tickers_with_gemini(anfrage)
    if funde:
        update_isin_pool(funde)
