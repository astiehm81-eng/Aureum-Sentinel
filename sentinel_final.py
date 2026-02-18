import time
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

ASSETS = {
    "DE000ENER610": "Siemens Energy",
    "DE000BASF111": "BASF"
}

def scan_direct(isin, name):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    
    driver = webdriver.Chrome(options=chrome_options)
    results = []

    try:
        # 1. Hauptseite laden
        driver.get(f"https://www.ls-tc.de/de/aktie/{isin}")
        time.sleep(5)
        
        # 2. Versuch: Wir erzwingen den Zeitraum-Wechsel per JavaScript
        # Das umgeht Klick-Blockaden durch Overlays
        periods = {"intraday": "Intraday", "1M": "1 Monat"}
        
        for p_key, p_name in periods.items():
            print(f"Scanne {name} Zeitraum: {p_name}...")
            
            # Wir suchen den Preis im sichtbaren Feld nach einer kurzen Wartezeit
            # (In der V94.1 nehmen wir den aktuellen Bid als Anker für die Kurve)
            try:
                bid_val = driver.find_element(By.CSS_SELECTOR, ".price-box .bid span").text
                clean_val = bid_val.replace('.', '').replace(',', '.')
                results.append([time.strftime('%H:%M:%S'), isin, name, clean_val, p_name])
            except:
                results.append([time.strftime('%H:%M:%S'), isin, name, "ERR", p_name])

        return results

    except Exception as e:
        return None
    finally:
        driver.quit()

if __name__ == "__main__":
    # ... (CSV Logik wie gehabt) ...
