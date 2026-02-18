import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random
from datetime import datetime, timedelta

# --- KONFIGURATION ---
CSV_FILE = "sentinel_market_data.csv"
RUNTIME_LIMIT = 900  # 15 Minuten Laufzeit
INTERVAL = 15        # Erhöht auf 15s, um bei der hohen URL-Anzahl stabil zu bleiben
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}

# Erweiterte Asset-Listen für vollständige Marktabdeckung
URLS = {
    "DAX": "https://www.ls-tc.de/de/index/dax",
    "MDAX": "https://www.ls-tc.de/de/index/mdax",
    "SDAX": "https://www.ls-tc.de/de/index/sdax",
    "TecDAX": "https://www.ls-tc.de/de/index/tecdax",
    "EuroStoxx50": "https://www.ls-tc.de/de/index/euro-stoxx-50",
    "NASDAQ100": "https://www.ls-tc.de/de/index/nasdaq-100",
    "DowJones": "https://www.ls-tc.de/de/index/dow-jones",
    "Krypto": "https://www.ls-tc.de/de/kryptowaehrungen",
    "Hot_Stocks": "https://www.ls-tc.de/de/aktien"
}

def clean_old_data():
    """Löscht Einträge älter als 48 Stunden (Eiserner Standard)."""
    if not os.path.exists(CSV_FILE): return
    threshold = datetime.now() - timedelta(hours=48)
    kept_rows = []
    try:
        with open(CSV_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                if datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S") > threshold:
                    kept_rows.append(row)
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(kept_rows)
        print(f"[*] Cleanup: {len(kept_rows)} Zeilen verbleiben.")
    except Exception as e: print(f"Cleanup-Fehler: {e}")

def fetch_inflated_data(session):
    """Scrapt alle Sektoren für maximale Markttiefe."""
    market_results = []
    news_snippets = []
    
    for category, url in URLS.items():
        try:
            # Bot-Schutz Jitter
            time.sleep(random.uniform(0.01, 0.03))
            res = session.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'lxml' if 'lxml' in str(BeautifulSoup) else 'html.parser')
            
            # Kurse extrahieren
            for row in soup.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) >= 5:
                    try:
                        name = cols[0].text.strip().replace("\n", "")
                        bid = float(cols[3].text.strip().replace('.', '').replace(',', '.'))
                        ask = float(cols[4].text.strip().replace('.', '').replace(',', '.'))
                        spread = round(ask - bid, 4)
                        market_results.append([name, bid, ask, spread, category])
                    except: continue
            
            # News am Seitenende
            for tag in soup.select(".headline, .news-item, h3")[:5]:
                txt = tag.get_text(strip=True)
                if len(txt) > 30: news_snippets.append(txt[:150])
                
        except Exception as e:
            print(f"Fehler bei {category}: {e}")
            
    return market_results, list(set(news_snippets))

def run():
    start_time = time.time()
    session = requests.Session() # Session für Performance-Boost
    
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(["Timestamp", "Asset", "Bid", "Ask", "Spread", "Category", "News"])

    print(f"[*] Aureum Sentinel Inflator gestartet (Breite: {len(URLS)} Listen)")

    while (time.time() - start_time) < RUNTIME_LIMIT:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data, news = fetch_inflated_data(session)
        
        summary = " | ".join(news[:3])
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for row in data:
                writer.writerow([ts] + row + [summary])
                summary = "" # News nur einmal pro Block
        
        print(f"[*] {ts}: {len(data)} Assets geloggt.")
        time.sleep(INTERVAL)

    clean_old_data()

if __name__ == "__main__":
    run()
