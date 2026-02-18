import time
import csv
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Validierte ISINs für Siemens Energy (Ziel: 161,xx) und BASF (Ziel: 50,74)
ASSETS = {
    "DE000ENER610": "Siemens Energy",
    "DE000BASF111": "BASF",
    "DE000SAPG003": "SAP",
    "DE0005190003": "BMW"
}

CSV_FILE = 'sentinel_history.csv'

def scan_asset(isin, name):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=chrome_options)
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Direkter Aufruf der Asset-Seite
        driver.get(f"https://www.ls-tc.de/de/aktie/{isin}")
        time.sleep(7) # Wartezeit für das Rendering der Kurve
        
        # Extraktion des Preises direkt aus dem DOM
        # Wir nehmen den Bid-Preis, der auch die Kurve (Intraday) definiert
        price_element = driver.find_element(By.CSS_SELECTOR, ".price-box .bid span")
        raw_price = price_element.text
        clean_price = raw_price.replace('.', '').replace(',', '.')
        
        print(f"✅ {name}: {clean_price} € erfasst.")
        return [timestamp, isin, name, clean_price, "Intraday"]
        
    except Exception as e:
        print(f"❌ Fehler bei {name}: {str(e)[:50]}")
        return [timestamp, isin, name, "0.00", "ERROR"]
    finally:
        driver.quit()

if __name__ == "__main__":
    # 1. CSV Initialisierung
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'ISIN', 'Asset', 'Price', 'Type'])

    # 2. Sequentieller Scan zur Vermeidung von Memory-Overload auf GitHub
    all_data = []
    for isin, asset_name in ASSETS.items():
        result = scan_asset(isin, asset_name)
        if result:
            all_data.append(result)

    # 3. Daten wegschreiben
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(all_data)
    
    print(f"🏁 Sentinel-Run beendet. {len(all_data)} Werte gesichert.")
