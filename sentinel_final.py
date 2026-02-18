import time
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

ASSETS = {
    "BASF11": "BASF",
    "ENER61": "Siemens Energy",
    "SAP000": "SAP",
    "BMW111": "BMW"
}

def vision_worker(wkn, name):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
        # Wir geben der Seite massiv Zeit zum Rendern (wie ein Mensch)
        time.sleep(10) 
        
        # Screenshot der gesamten Preis-Sektion
        screenshot_path = f"debug_{wkn}.png"
        driver.save_screenshot(screenshot_path)
        
        # Wir ziehen den Text jetzt stumpf aus dem BODY, ohne Selektoren-Logik
        full_text = driver.find_element(By.TAG_NAME, "body").text
        
        # Wir suchen im Text-Block nach der Preis-Struktur (Geld / Brief)
        # Das ist genau das, was du auf deinem Foto siehst
        lines = full_text.split('\n')
        found_price = "0.00"
        
        for i, line in enumerate(lines):
            if "Geld" in line or "Bid" in line:
                # Oft steht der Preis in der nächsten Zeile
                found_price = lines[i+1] if i+1 < len(lines) else "Fehler"
                break

        print(f"👁️ Vision-Ergebnis für {name}: {found_price}")
        return [time.strftime('%H:%M:%S'), wkn, name, found_price]

    except Exception as e:
        return None
    finally:
        driver.quit()

# ... (CSV Handling wie gehabt) ...
