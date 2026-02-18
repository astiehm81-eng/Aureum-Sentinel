import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import sys
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Konfiguration
CSV_FILE = "sentinel_market_data.csv"
RUNTIME_LIMIT_SECONDS = 1200  
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.ls-tc.de/de/"
}

INDEX_URLS = {
    "DAX_Einzel": "https://www.ls-tc.de/de/index/dax",
    "NASDAQ_Einzel": "https://www.ls-tc.de/de/index/nasdaq-100",
    "Krypto": "https://www.ls-tc.de/de/kryptowaehrungen"
}

# Globale Session für Cookie-Handling (wirkt menschlicher)
session = requests.Session()
session.headers.update(HEADERS)

def fetch_list(url):
    extracted = []
    try:
        res = session.get(url, timeout=5)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, 'html.parser')
            for row in soup.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) >= 3:
                    name = cols[0].text.strip().replace("\n", " ")
                    price = cols[2].text.strip().replace('.', '').replace(',', '.')
                    if any(char.isdigit() for char in price):
                        extracted.append([name, price])
    except: pass
    return extracted

def clean_old_data():
    if not os.path.exists(CSV_FILE): return
    cutoff = datetime.now() - timedelta(days=2)
    rows_to_keep = []
    try:
        with open(CSV_FILE, mode='r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if datetime.strptime(row['Timestamp'], "%Y-%m-%d %H:%M:%S.%f") > cutoff:
                    rows_to_keep.append(row)
        with open(CSV_FILE, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["Timestamp", "Asset", "Ask_Price"])
            writer.writeheader()
            writer.writerows(rows_to_keep)
    except: pass

def run_sentinel():
    start_time = time.time()
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "Asset", "Ask_Price"])

    print(f"[*] Sentinel V42 aktiv. Nutze Session-Persistenz & Anti-Block Jitter.", flush=True)

    while (time.time() - start_time) < RUNTIME_LIMIT_SECONDS:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(fetch_list, INDEX_URLS.values()))
        
        with open(CSV_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            for sublist in results:
                for name, price in sublist:
                    writer.writerow([ts, name, price])
        
        # Der 1ms Basis-Delay + kleiner Jitter gegen Blocking
        # Das verhindert, dass die Firewall ein exaktes Zeitmuster erkennt
        time.sleep(0.001 + (random.uniform(0, 0.009))) 
        
        # Konsolen-Feedback
        if random.random() > 0.95: # Nur bei ~5% der Scans loggen, um GitHub-Log klein zu halten
            print(f"[{ts[:19]}] Monitoring aktiv... ({sum(len(x) for x in results)} Assets)", flush=True)

    clean_old_data()
    print("[*] Intervall beendet. Daten werden committet.", flush=True)

if __name__ == "__main__":
    run_sentinel()
