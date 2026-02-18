import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random
from datetime import datetime

# --- TEST-KONFIGURATION ---
CSV_FILE = "sentinel_market_data.csv"
RUNTIME_LIMIT = 60  # Nur 1 Minute für den Test
INTERVAL = 5        # Kürzerer Intervall für den Test
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}

URLS = {
    "DAX": "https://www.ls-tc.de/de/index/dax",
    "NASDAQ100": "https://www.ls-tc.de/de/index/nasdaq-100",
    "Krypto": "https://www.ls-tc.de/de/kryptowaehrungen"
} # Reduziert auf 3 Listen für den schnellen Test

def fetch_data_with_micro_delay(url, category):
    results = []
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code != 200: return []
        soup = BeautifulSoup(res.text, 'html.parser')
        rows = soup.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 5:
                time.sleep(0.001) # Dein 1ms Micro-Delay pro Asset
                try:
                    name = cols[0].get_text(strip=True)
                    bid = float(cols[3].get_text(strip=True).replace('.', '').replace(',', '.'))
                    ask = float(cols[4].get_text(strip=True).replace('.', '').replace(',', '.'))
                    results.append([name, bid, ask, round(ask-bid, 4), category])
                except: continue
    except: pass
    return results

def run_test():
    start_time = time.time()
    print(f"[*] TESTLAUF: Aureum Sentinel (60s) gestartet...")
    
    if not os.path.exists(CSV_FILE) or os.stat(CSV_FILE).st_size == 0:
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(["Timestamp", "Asset", "Bid", "Ask", "Spread", "Category"])

    while (time.time() - start_time) < RUNTIME_LIMIT:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = []
        
        for cat, url in URLS.items():
            page_data = fetch_data_with_micro_delay(url, cat)
            all_data.extend(page_data)
            time.sleep(0.5) # Kurze Pause zwischen den Listen
        
        if all_data:
            with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for row in all_data:
                    writer.writerow([ts] + row)
                f.flush() # Sofortiges Schreiben
                os.fsync(f.fileno()) 
            print(f"[*] {ts}: {len(all_data)} Assets im Test geloggt.")
        
        time.sleep(INTERVAL)
    print("[*] TEST beendet. Prüfe jetzt den GitHub-Commit.")

if __name__ == "__main__":
    run_test()
