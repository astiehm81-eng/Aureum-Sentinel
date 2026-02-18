import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random
from datetime import datetime, timedelta

CSV_FILE = "sentinel_market_data.csv"
RUNTIME_LIMIT = 900 
INTERVAL = 15 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}

# Vollständige Abdeckung wie gewünscht
URLS = {
    "DAX": "https://www.ls-tc.de/de/index/dax",
    "MDAX": "https://www.ls-tc.de/de/index/mdax",
    "SDAX": "https://www.ls-tc.de/de/index/sdax",
    "TecDAX": "https://www.ls-tc.de/de/index/tecdax",
    "NASDAQ100": "https://www.ls-tc.de/de/index/nasdaq-100",
    "DowJones": "https://www.ls-tc.de/de/index/dow-jones",
    "Krypto": "https://www.ls-tc.de/de/kryptowaehrungen",
    "Hot_Stocks": "https://www.ls-tc.de/de/aktien"
}

def fetch_data_with_micro_delay(url, category):
    results = []
    try:
        res = requests.get(url, headers=HEADERS, timeout=12)
        if res.status_code != 200: return []
        
        soup = BeautifulSoup(res.text, 'html.parser')
        rows = soup.find_all("tr")
        
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 5:
                # --- DER ENTSCHEIDENDE 1MS DELAY PRO ASSET ---
                time.sleep(0.001) 
                
                try:
                    name = cols[0].get_text(strip=True)
                    bid = float(cols[3].get_text(strip=True).replace('.', '').replace(',', '.'))
                    ask = float(cols[4].get_text(strip=True).replace('.', '').replace(',', '.'))
                    results.append([name, bid, ask, round(ask-bid, 4), category])
                except: continue
    except: pass
    return results

def run():
    start_time = time.time()
    print(f"[*] Aureum Sentinel V52 gestartet. 1ms Micro-Delay aktiv.")
    
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(["Timestamp", "Asset", "Bid", "Ask", "Spread", "Category"])

    while (time.time() - start_time) < RUNTIME_LIMIT:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = []
        
        # Gehe die Listen durch
        for cat, url in URLS.items():
            # Kurze Pause zwischen den Listen (Server-Schutz)
            time.sleep(random.uniform(0.5, 1.0))
            page_data = fetch_data_with_micro_delay(url, cat)
            all_data.extend(page_data)
        
        if all_data:
            with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for row in all_data:
                    writer.writerow([ts] + row)
            print(f"[*] {ts}: {len(all_data)} Assets erfolgreich mit Micro-Delay geloggt.")
        
        time.sleep(INTERVAL)

if __name__ == "__main__":
    run()
