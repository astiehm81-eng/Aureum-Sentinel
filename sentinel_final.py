import time
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

ASSETS = {
    "DE000ENER610": "Siemens Energy",
    "DE000BASF111": "BASF"
}

def scan_curve(isin, name):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    
    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 15)
    results = []

    try:
        driver.get(f"https://www.ls-tc.de/de/aktie/{isin}")
        
        # Zeiträume die wir nacheinander durchschalten
        # Die IDs/Texte entsprechen den Buttons "Intraday", "1 Monat"
        periods = ["Intraday", "1 Monat"]
        
        for period in periods:
            # 1. Button finden und klicken (Steuerung der Kurve)
            button = wait.until(EC.element_to_be_clickable((By.XPATH, f"//button[contains(text(), '{period}')]")))
            driver.execute_script("arguments[0].click();", button)
            time.sleep(3) # Zeit für den Chart-Wechsel
            
            # 2. Den aktuellsten Wert aus der nun aktiven Kurve ziehen
            # Wir suchen den Kurs im Highcharts-Container oder dem Preis-Feld
            price_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".price-box .bid span")))
            current_val = price_element.text.replace('.', '').replace(',', '.')
            
            results.append([time.strftime('%Y-%m-%d %H:%M:%S'), isin, name, current_val, period])
            print(f"✅ {name} ({period}): {current_val}")

        return results

    except Exception as e:
        print(f"❌ Fehler beim Kurven-Scan ({name}): {e}")
        return None
    finally:
        driver.quit()

if __name__ == "__main__":
    # CSV Reset
    with open('sentinel_history.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'ISIN', 'Asset', 'Price', 'Period'])

    for isin, name in ASSETS.items():
        data = scan_curve(isin, name)
        if data:
            with open('sentinel_history.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(data)
