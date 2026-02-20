import os
import json
import sys
from datetime import datetime
from google import genai

# --- KONFIGURATION ---
api_key = os.getenv("GEMINI_API_KEY")
POOL_FILE = "isin_pool.json"

def log(msg):
    print(f"🔍 [FINDER] {datetime.now().strftime('%H:%M:%S')} - {msg}")

if not api_key:
    log("ERROR: Kein API_KEY gefunden. Bitte GitHub Secrets prüfen.")
    sys.exit(0)

# Initialisierung des neuen Clients
client = genai.Client(api_key=api_key)

def get_market_segment():
    # Rotiert stündlich durch verschiedene Sektoren für maximale Abdeckung
    segments = [
        "S&P 500 & Nasdaq 100 Tech-Giganten", 
        "DAX, MDAX, SDAX & EuroStoxx 600",
        "Top 200 Crypto & DeFi Tokens (USD)",
        "Nikkei 225 & Asiatische Bluechips",
        "Rohstoff-Aktien & Energie-Sektor (Oil/Gas)",
        "Finanzsektor & Banken weltweit",
        "Healthcare & Emerging Markets"
    ]
    return segments[datetime.now().hour % len(segments)]

def search_massive():
    segment = get_market_segment()
    log(f"Fokussiere Segment: {segment}")
    
    prompt = (
        f"Erstelle eine Liste von 150 Yahoo Finance Tickern für: {segment}. "
        "Antworte AUSSCHLIESSLICH im JSON-Format als Array von Objekten: "
        "[{\"symbol\": \"AAPL\"}, {\"symbol\": \"TSLA\"}, ...]"
    )
    
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        text = response.text.strip()
        # Extraktion falls Gemini Markdown-Code-Blocks verwendet
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        
        return json.loads(text)
    except Exception as e:
        log(f"Fehler bei Gemini-Abfrage: {str(e)[:100]}")
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
            # Wir speichern ihn erstmal als 'active'. Validierung erfolgt im Sentinel Puls.
            pool.append({'symbol': sym, 'added_at': datetime.now().isoformat()})
            existing.add(sym)
            added += 1

    with open(POOL_FILE, "w") as f:
        json.dump(pool, f, indent=4)
    log(f"POOL-UPDATE: +{added} Assets. Gesamtbestand: {len(pool)}")

if __name__ == "__main__":
    verify_and_update()
