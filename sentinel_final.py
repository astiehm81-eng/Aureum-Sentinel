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

def vision_worker(wkn):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080") # Größeres Fenster für Chart-Rendering
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=chrome_options)
    base_url = f"https://www.ls-tc.de/de/aktie/{wkn}"
    
    try:
        driver.get(base_url)
        # 1ms Delay Simulation zwischen Tabs
        time.sleep(0.001)

        # Selektor-Anpassung: L&S nutzt oft span oder div mit spezifischen Klassen
        # Wir warten bis das Preis-Element sichtbar ist (Timeout 10s)
        wait = WebDriverWait(driver, 10)
        
        # Versuche den aktuellen Kurs zu finden (angepasster Selektor für L&S)
        price_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.price span, .instrument-price span")))
        
        periods = ["1d", "1w", "max"]
        extracted_data = []

        for period in periods:
            print(f"📡 Worker {wkn}: Analysiere Chart '{period}'")
            
            # Button-Klick Simulation (falls Buttons vorhanden sind)
            try:
                # Suche Buttons wie '1 Tag', '1 Woche', etc.
                btn = driver.find_element(By.XPATH, f"//button[contains(text(), '{period}')] | //a[contains(text(), '{period}')]")
                btn.click()
                time.sleep(0.001) # 1ms nach Klick
            except:
                pass # Falls Button nicht direkt klickbar, bleib beim aktuellen View

            current_price = price_element.text.replace('.', '').replace(',', '.')
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            extracted_data.append([timestamp, wkn, current_price, period])

        return extracted_data
    except Exception as e:
        print(f"❌ Vision-Fehler bei {wkn}: Element nicht gefunden (Timeout)")
        return []
    finally:
        driver.quit()

if __name__ == "__main__":
    print("🚀 Aureum Sentinel V89.1: Multi-Worker Vision Re-Start...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(vision_worker, ASSETS))
    
    flat_results = [item for sublist in results for item in sublist]
    if flat_results:
        with open('sentinel_history.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(flat_results)
        print(f"🏁 {len(flat_results)} Bild-Datenpunkte gesichert.")
