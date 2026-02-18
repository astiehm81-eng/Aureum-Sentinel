import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

CSV_FILE = "sentinel_market_data.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

# URLs für Massen-Scraping
INDEX_URLS = {
    "DAX_Einzel": "https://www.ls-tc.de/de/index/dax",
    "NASDAQ_Einzel": "https://www.ls-tc.de/de/index/nasdaq-100",
    "Krypto": "https://www.ls-tc.de/de/kryptowaehrungen"
}

def clean_old_data():
    """Entfernt alle Einträge, die älter als 48 Stunden sind."""
    if not os.path.exists(CSV_FILE):
        return
    
    cutoff = datetime.now() - timedelta(days=2)
    rows_to_keep = []
    
    try:
        with open(CSV_FILE, mode='r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Zeitstempel parsen und mit Cutoff vergleichen
                row_time = datetime.strptime(row['Timestamp'], "%Y-%m-%d %H:%M:%S.%f")
                if row_time > cutoff:
                    rows_to_keep.append(row)
        
        # CSV mit bereinigten Daten überschreiben
        with open(CSV_FILE, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["Timestamp", "Asset", "Ask_Price"])
            writer.writeheader()
            writer.writerows(rows_to_keep)
    except Exception as e:
        print(f"[!] Fehler bei Datenbereinigung: {e}", flush=True)

def fetch_list(url):
    extracted = []
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        rows = soup.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 3:
                name = cols[0].text.strip().replace("\n", " ")
                price = cols[2].text.strip().replace('.', '').replace(',', '.')
                if any(char.isdigit() for char in price):
                    extracted.append([name, price])
    except:
        pass
    return extracted

def run_sentinel():
    # Initialer Header, falls Datei nicht existiert
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "Asset", "Ask_Price"])

    print("[*] Aureum Sentinel 48h-Rolling-Logger gestartet...", flush=True)

    counter = 0
    while True:
        ts_now = datetime.now()
        ts_str = ts_now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # 1. Daten abrufen
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(fetch_list, INDEX_URLS.values()))
        
        # 2. In CSV anhängen
        with open(CSV_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            for sublist in results:
                for name, price in sublist:
                    writer.writerow([ts_str, name, price])
        
        # 3. Alle 100 Durchläufe (ca. alle 5-10 Min) alte Daten löschen
        counter += 1
        if counter >= 100:
            clean_old_data()
            print(f"[{ts_str}] Datenbereinigung durchgeführt (2-Tage-Fenster).", flush=True)
            counter = 0

        # Feedback im Log
        print(f"[{ts_str}] Scan OK. {sum(len(x) for x in results)} Assets erfasst.", flush=True)
        sys.stdout.flush()
        
        time.sleep(2) # Kurze Pause zur Stabilisierung

if __name__ == "__main__":
    run_sentinel()
