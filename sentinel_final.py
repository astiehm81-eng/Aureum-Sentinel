import requests
from bs4 import BeautifulSoup
import csv
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

CSV_FILE = "sentinel_market_data.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

# Definition der Hub-URLs für Massen-Scraping
INDEX_URLS = {
    "DAX_All": "https://www.ls-tc.de/de/index/dax",
    "NASDAQ_All": "https://www.ls-tc.de/de/index/nasdaq-100",
    "Crypto": "https://www.ls-tc.de/de/kryptowaehrungen"
}

# Einzel-Assets für direkten Fokus
CORE_ASSETS = {
    "Bitcoin": "DE000A28M8D0", # BTC ETC als Proxy oder Direktlink
    "Siemens_Energy": "DE000ENER6Y0",
    "SAP_SE": "DE0007164600"
}

def fetch_table_data(url):
    """Extrahiert alle Kurse aus einer Tabellenübersicht (Index-Seite)."""
    assets = []
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        # L&S Tabellenstruktur: Suche nach Zeilen in der Kursliste
        rows = soup.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 3:
                name = cols[0].text.strip().replace("\n", "")
                # Der Briefkurs steht bei Indexlisten oft in einer spezifischen Spalte
                price = cols[2].text.strip().replace('.', '').replace(',', '.')
                if price and any(char.isdigit() for char in price):
                    assets.append([name, price])
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return assets

def run_sentinel_max():
    if os.path.exists(CSV_FILE): os.remove(CSV_FILE)
    
    with open(CSV_FILE, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "Asset", "Price"])

    print(f"[*] Aureum Sentinel Max gestartet. Scanne DAX, NASDAQ & BTC...")

    while True:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        all_results = []

        # 1. Scrape Index-Listen (Massenabfrage)
        with ThreadPoolExecutor(max_workers=5) as executor:
            lists = list(executor.map(fetch_table_data, INDEX_URLS.values()))
            for l in lists: all_results.extend(l)

        # 2. Schreibe in CSV
        with open(CSV_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            for name, price in all_results:
                writer.writerow([ts, name, price])
                # Filter für die Konsole, damit das Log nicht explodiert
                if "Siemens" in name or "Bitcoin" in name or "SAP" in name:
                    print(f"[{ts}] {name}: {price} €")

        # Da wir jetzt ca. 150 Werte pro Durchgang holen, erhöhen wir das Intervall leicht
        time.sleep(1)

if __name__ == "__main__":
    run_sentinel_max()
