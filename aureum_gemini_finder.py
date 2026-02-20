import google.generativeai as genai
import json
import os
import yfinance as yf
import sys
from datetime import datetime

# --- KONFIGURATION V110 ---
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-pro')
POOL_FILE = "isin_pool.json"

def log(msg):
    sys.stdout.write(f"🔍 [FINDER] {msg}\n")
    sys.stdout.flush()

def get_market_segment():
    """Wählt je nach aktueller Stunde ein anderes Marktsegment für die Suche."""
    hour = datetime.now().hour
    segments = [
        "S&P 500 & Nasdaq 100 Tech-Giganten",
        "DAX, MDAX, SDAX & EuroStoxx 600",
        "Top 100 Crypto & DeFi Tokens (USD)",
        "Nikkei 225 & Asiatische Bluechips",
        "Rohstoff-Aktien & Energie-Sektor (Oil/Gas)",
        "Finanzsektor & Banken weltweit"
    ]
    return segments[hour % len(segments)]

def search_massive():
    segment = get_market_segment()
    log(f"Fokussiere Segment: {segment}")
    
    prompt = (
        f"Erstelle eine Liste von 150 Yahoo Finance Tickern für: {segment}. "
        "Antworte AUSSCHLIESSLICH im JSON-Format als Array von Objekten: "
        "[{\"symbol\": \"AAPL\"}, {\"symbol\": \"TSLA\"}, ...]"
    )
    
    try:
        response = model.generate_content(prompt)
        # Säuberung des Outputs von Markdown-Blöcken
        text = response.text.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        
        return json.loads(text)
    except Exception as e:
        log(f"Fehler bei Gemini-Abfrage: {e}")
        return []

def verify_and_update():
    new_found = search_massive()
    if not new_found: return

    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, "r") as f: pool = json.load(f)
    else: pool = []
    
    existing = {a['symbol'] for a in pool}
    added = 0
    
    for f in new_found:
        sym = f['symbol'].upper()
        if sym not in existing:
            # Schneller Check, ob Ticker existiert
            try:
                t = yf.Ticker(sym)
                if not t.history(period="1d").empty:
                    pool.append({'symbol': sym, 'added_at': datetime.now().isoformat()})
                    existing.add(sym)
                    added += 1
                    log(f"✅ NEU ENTDECKT: {sym}")
            except: continue

    with open(POOL_FILE, "w") as f:
        json.dump(pool, f, indent=4)
    log(f"POOL-UPDATE: +{added} Assets. Gesamtbestand im Sentinel: {len(pool)}")

if __name__ == "__main__":
    verify_and_update()
