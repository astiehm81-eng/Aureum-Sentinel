import time
import csv
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

ASSETS = ["ENER61", "SAP000", "A1EWWW", "A0AE1X", "BASF11", "DTE000", "VOW300", 
          "ADS000", "DBK100", "ALV001", "BAY001", "BMW111", "IFX000", "MUV200"]

def force_vision_worker(wkn):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
        time.sleep(2) # Kurze Zeit zum Laden der JS-Elemente

        # 1. Brutaler Cookie-Kill (entfernt das Overlay einfach aus dem DOM)
        driver.execute_script("""
            var elements = document.querySelectorAll('[class*="consent"], [id*="cookie"], [class*="modal"]');
            for(var i=0; i<elements.length; i++){ elements[i].remove(); }
        """)

        # 2. Die Buttons ansteuern (Intraday, 1 Monat, Alles)
        # Wir nutzen JS-Klicks, um 'Message: element not interactable' zu umgehen
        periods = ["Intraday", "1 Monat", "Alles"]
        extracted_results = []

        for period in periods:
            print(f"📡 Worker {wkn}: Erfasse Grafik '{period}'...")
            
            # Suche das Element und klicke es via JavaScript
            script = f"""
                var items = document.querySelectorAll('ul.chart-tabs li, .chart-periods a, button');
                for (var i = 0; i < items.length; i++) {{
                    if (items[i].textContent.includes('{period}')) {{
                        items[i].click();
                        return true;
                    }}
                }}
                return false;
            """
            success = driver.execute_script(script)
            
            # Dein 1ms Delay (0.001s)
            time.sleep(0.001)
            
            # Preis-Extraktion: Wir nehmen den Geldkurs, den man im Screenshot sieht
            try:
                price = driver.execute_script("return document.querySelector('.price-box .bid span')?.innerText || '0'")
                clean_price = price.replace('.', '').replace(',', '.')
                extracted_results.append([time.strftime('%Y-%m-%d %H:%M:%S'), wkn, clean_price, period])
            except:
                pass

        return extracted_results

    except Exception as e:
        print(f"❌ Fehler bei {wkn}: {e}")
        return []
    finally:
        driver.quit()

if __name__ == "__main__":
    print("🚀 V90.1: Starte JS-Force-Engine (Keine Blockaden mehr)...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(force_vision_worker, ASSETS))
    
    # Ergebnisse wegschreiben
    with open('sentinel_history.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        for res in [r for r in results if r]:
            writer.writerows(res)
    print(f"🏁 Mission abgeschlossen. Historie für {len(ASSETS)} Assets synchronisiert.")
