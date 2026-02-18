import time
import csv
import os
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Definierte Assets mit exakten Namens-Ankern für die Validierung
ASSETS = {
    "BASF11": "BASF",
    "ENER61": "Siemens Energy",
    "SAP000": "SAP",
    "BMW111": "BMW",
    "ALV001": "Allianz",
    "DTE000": "Telekom",
    "VOW300": "Volkswagen",
    "A0AE1X": "Nasdaq"
}

CSV_FILE = 'sentinel_history.csv'

def stealth_worker(wkn, expected_name):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Stealth User-Agent gegen Bot-Detection
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 15)
    
    try:
        # 1. Tarnungs-Anlauf
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
        
        # 2. Namens-Validierung (Der wichtigste Schutz gegen "Unsinn")
        # Wir warten, bis der Titel geladen ist
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
        actual_title = driver.title
        
        if expected_name.lower() not in actual_title.lower():
            print(f"❌ Validierungsfehler: Seite für {wkn} zeigt '{actual_title}' statt {expected_name}")
            return None

        # 3. Preis-Extraktion via XPATH (Sucht das Label 'Geld' und nimmt den Preis daneben)
        # Dieser Pfad ist resistent gegen Design-Änderungen der CSS-Klassen
        bid_xpath = "//div[contains(@class, 'price-box')]//div[contains(., 'Geld')]/following-sibling::div/span"
        bid_element = wait.until(EC.visibility_of_element_located((By.XPATH, bid_xpath)))
        
        raw_price = bid_element.text
        clean_price = raw_price.replace('.', '').replace(',', '.')
        
        print(f"✅ {expected_name}: {clean_price} € erfasst.")
        
        return [time.strftime('%Y-%m-%d %H:%M:%S'), wkn, expected_name, clean_price, "Intraday"]

    except Exception as e:
        print(f"⚠️ Worker-Fehler bei {expected_name} ({wkn}): Seite evtl. geblockt.")
        return None
    finally:
        driver.quit()

if __name__ == "__main__":
    # Schritt 1: CSV leeren und Header schreiben
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'WKN', 'Asset', 'Price', 'Type'])

    print("🚀 Aureum Sentinel V91.2: Starte validierten Stealth-Run...")
    
    # Parallele Ausführung
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(lambda x: stealth_worker(x[0], x[1]), ASSETS.items()))

    # Ergebnisse filtern und sichern
    valid_data = [r for r in results if r is not None]
    
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(valid_data)
    
    print(f"🏁 Fertig. {len(valid_data)} Assets sauber in {CSV_FILE} übertragen.")
