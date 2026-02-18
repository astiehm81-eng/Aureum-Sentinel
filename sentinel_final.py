import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random
from datetime import datetime, timedelta

# --- KONFIGURATION ---
CSV_FILE = "sentinel_market_data.csv"
RUNTIME_LIMIT = 900  # 15 Minuten Laufzeit pro Start
INTERVAL = 10        # Alle 10 Sekunden ein Scan (Breite vor Geschwindigkeit)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Ziel-URLs für maximale Abdeckung (L&S Handelsplatz)
URLS = {
    "DAX": "https://www.ls-tc.de/de/index/dax",
    "NASDAQ": "https://www.ls-tc.de/de/index/nasdaq-100",
    "Krypto": "https://www.ls-tc.de/de/kryptowaehrungen",
    "Top_Aktien": "https://www.ls-tc.de/de/aktien"
}

def clean_old_data():
    """Löscht alle Einträge, die älter als 48 Stunden sind (Eiserner Standard)."""
    if not os.path.exists(CSV_FILE): return
    
    threshold = datetime.now() - timedelta(hours=48)
    kept_rows = []
    
    with open(CSV_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            try:
                row_dt = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                if row_dt > threshold:
                    kept_rows.append(row)
            except: continue
            
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(kept_rows)
    print(f"[*] Cleanup abgeschlossen. {len(kept_rows)} Ticks verbleiben.")

def fetch_data():
    """Scrapt Kurse, Spreads und News-Texte."""
    market_results = []
    news_snippets = []
    
    for category, url in URLS.items():
        try:
            # Bot-Schutz Jitter (1ms bis 15ms)
            time.sleep(0.001 + random.uniform(0.005, 0.015))
            
            res = requests.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # 1. Kurs- & Spread-Extraktion
            for row in soup.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) >= 5:
                    try:
                        name = cols[0].text.strip()
                        bid = float(cols[3].text.strip().replace('.', '').replace(',', '.'))
                        ask = float(cols[4].text.strip().replace('.', '').replace(',', '.'))
                        spread = round(ask - bid, 4)
                        market_results.append([name, bid, ask, spread, category])
                    except: continue
            
            # 2. News-Texte am Seitenende (Sektor-Memory Input)
            news_tags = soup.select(".headline, .news-item, h3")
            for tag in news_tags:
                txt = tag.get_text(strip=True)
                if len(txt) > 30: news_snippets.append(txt[:180])
                
        except Exception as e:
            print(f"Fehler bei {category}: {e}")
            
    return market_results, list(set(news_snippets)) # Set entfernt Dubletten

def run():
    start_time = time.time()
    
    # CSV Header falls neu
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(["Timestamp", "Asset", "Bid", "Ask", "Spread", "Category", "News"])

    while (time.time() - start_time) < RUNTIME_LIMIT:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data, news = fetch_data()
        
        # News zusammenfassen (Top 3 für Sektor-Analyse)
        summary = " | ".join(news[:3])
        
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for row in data:
                writer.writerow([ts] + row + [summary])
                summary = "" # News nur einmal pro Block schreiben
        
        print(f"[*] {ts}: {len(data)} Assets erfasst.")
        time.sleep(INTERVAL)

    # Am Ende des 15-Minuten-Runs: Cleanup
    clean_old_data()

if __name__ == "__main__":
    run()
