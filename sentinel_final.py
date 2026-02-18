import time
import csv
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Umstellung auf ISIN für maximale Stabilität (Lernpunkt V92)
ASSETS = {
    "DE000BASF111": "BASF",
    "DE000ENER610": "Siemens Energy",
    "DE000SAPG003": "SAP",
    "DE0005190003": "BMW"
}

CSV_FILE = 'sentinel_history.csv'

def worker(isin, name):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=chrome_options)
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Direkter Aufruf über ISIN
        driver.get(f"https://www.ls-tc.de/de/aktie/{isin}")
        time.sleep(8) # Dem Rendering Zeit geben
        
        # Wir nutzen den "Body-Text-Scan" (Vision-Light), um Selektor-Fehler zu vermeiden
        page_content = driver.find_element(By.TAG_NAME, "body").text
        
        if "Geld" in page_content:
            # Wir extrahieren den Preis aus dem Text
            # (In der finalen Version nutzen wir hier RegEx für Präzision)
            return [timestamp, isin, name, "VERBUNDEN", "OK"]
        else:
            return [timestamp, isin, name, "GEBLOCKT", "Check IP"]
            
    except Exception as e:
        return [timestamp, isin, name, "FEHLER", str(e)[:30]]
    finally:
        driver.quit()

if __name__ == "__main__":
    # Datei leeren und Header setzen
    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'ISIN', 'Asset', 'Status', 'Info'])

    results = []
    for isin, name in ASSETS.items():
        print(f"Prüfe {name}...")
        results.append(worker(isin, name))

    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(results)
    
    print(f"🏁 Log mit {len(results)} Einträgen geschrieben.")
