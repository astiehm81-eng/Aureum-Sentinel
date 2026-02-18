import time
import csv
import os
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Konfiguration der Assets mit Namens-Check zur Validierung
ASSETS = {
    "BASF11": "BASF",
    "ENER61": "Siemens Energy",
    "SAP000": "SAP",
    "A1EWWW": "Adidas",
    "A0AE1X": "Nasdaq",
    "DTE000": "Telekom",
    "VOW300": "Volkswagen",
    "DBK100": "Deutsche Bank",
    "ALV001": "Allianz",
    "BAY001": "Bayer",
    "BMW111": "BMW",
    "IFX000": "Infineon",
    "MUV200": "Muenchener Rueck",
    "A0D655": "Nordex"
}

CSV_FILE = 'sentinel_history.csv'

def validated_worker(wkn, expected_name):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
        time.sleep(2) # Warten auf JS-Rendering

        # 1. Validierung des Titels (Verhindert WKN-Vertauschung)
        page_title = driver.find_element(By.TAG_NAME, "h1").text
        if expected_name.lower() not in page_title.lower():
            print(f"⚠️ WKN-Mismatch bei {wkn}: Seite zeigt '{page_title}'")
            return None

        # 2. Präzise Extraktion von Geld (Bid) und Brief (Ask)
        # Wir nutzen spezifischere Selektoren basierend auf dem L&S Layout
        try:
            bid = driver.find_element(By.CSS_SELECTOR, "div.price-box .bid span").text
            ask = driver.find_element(By.CSS_SELECTOR, "div.price-box .ask span").text
            
            clean_bid = bid.replace('.', '').replace(',', '.')
            clean_ask = ask.replace('.', '').replace(',', '.')
            
            # 3. Intraday-Punkt erfassen
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            return [timestamp, wkn, expected_name, clean_bid, clean_ask, "Intraday"]
        except Exception as e:
            print(f"❌ Preis-Extraktionsfehler bei {wkn}: {e}")
            return None

    except Exception as e:
        print(f"❌ Verbindungfehler bei {wkn}: {e}")
        return None
    finally:
        driver.quit()

if __name__ == "__main__":
    print(f"🧹 Debug-Mode: Leere {CSV_FILE}...")
    # CSV mit Header neu erstellen (löscht alten Inhalt)
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'WKN', 'Asset', 'Bid', 'Ask', 'Type'])

    print("🚀 Starte validierten Multi-Worker-Lauf (V91)...")
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda x: validated_worker(x[0], x[1]), ASSETS.items()))

    # Ergebnisse filtern und speichern
    valid_results = [r for r in results if r is not None]
    
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(valid_results)
    
    print(f"🏁 Validierung abgeschlossen. {len(valid_results)} saubere Datensätze gespeichert.")
