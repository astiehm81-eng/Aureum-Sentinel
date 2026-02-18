import time
import csv
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

ASSETS = ["ENER61", "SAP000", "A1EWWW", "A0AE1X", "BASF11", "DTE000", "VOW300", 
          "ADS000", "DBK100", "ALV001", "BAY001", "BMW111", "IFX000", "MUV200"]

def vision_tab_worker(wkn):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1080,1920") # Hochformat wie dein Screenshot
    
    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 15)
    
    try:
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
        
        # 1. Cookie-Banner/Zustimmung wegklicken (Verhindert Timeouts)
        try:
            consent_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Zustimmen') or contains(., 'Akzeptieren')]")))
            consent_btn.click()
            time.sleep(0.5)
        except: pass 

        # 2. Die Buttons aus deinem Bild ansteuern
        # Wir 'klicken' uns durch die Historie: Alles -> 1 Jahr -> Intraday
        periods = ["Alles", "1 Jahr", "Intraday"]
        extracted_results = []

        for period in periods:
            # Wir suchen exakt die Texte aus deinem Screenshot (Intraday, 1 Monat, etc.)
            tab_button = wait.until(EC.element_to_be_clickable((By.XPATH, f"//ul[contains(@class, 'chart-tabs')]//a[contains(text(), '{period}')] | //li[contains(text(), '{period}')]")))
            tab_button.click()
            
            # 1ms Delay nach dem Klick (menschlicher Rhythmus)
            time.sleep(0.001)
            
            # Preis aus dem Header (Geld/Brief) extrahieren
            price_box = driver.find_element(By.CSS_SELECTOR, ".price-box, .instrument-price").text
            # Extrahiere nur die Zahl (z.B. 80.6200 aus deinem Bild)
            clean_price = "".join([c for c in price_box.split('€')[0] if c.isdigit() or c in ',.']).replace(',', '.')
            
            extracted_results.append([time.strftime('%Y-%m-%d %H:%M:%S'), wkn, clean_price, period])

        return extracted_results

    except Exception as e:
        print(f"❌ Fehler bei {wkn}: Tab '{period}' nicht klickbar.")
        return []
    finally:
        driver.quit()

if __name__ == "__main__":
    print("🚀 V90: Starte Tab-Navigation basierend auf Screenshot...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(vision_tab_worker, ASSETS))
    
    # Speichern der extrahierten Grafik-Endpunkte
    with open('sentinel_history.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        for res in [r for r in results if r]:
            writer.writerows(res)
    print("🏁 Alle Tabs verarbeitet.")
