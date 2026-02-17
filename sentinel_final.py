import sys
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

print("🛡️ AUREUM SENTINEL V42.3 - SPREAD-READY")
sys.stdout.flush()

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--lang=de-DE")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    try:
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    except:
        return None

if __name__ == "__main__":
    # Liste deiner Ziel-WKNs
    target_wkns = ["ENER61", "SAP000", "BASF11", "DTE000", "VOW300"]
    
    driver = setup_driver()
    if driver:
        for wkn in target_wkns:
            try:
                driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
                time.sleep(5) # Etwas mehr Zeit für die Preis-Animation
                html = driver.page_source
                
                # Smarte Suche nach Preisen (verschiedene CSS-Klassen/Attribute)
                price = None
                patterns = [
                    r'data-price="([\d,.]+)"',
                    r'class="price">([\d,.]+)</span>',
                    r'itemprop="price" content="([\d,.]+)"'
                ]
                
                for p in patterns:
                    match = re.search(p, html)
                    if match:
                        price = match.group(1)
                        break
                
                if price:
                    print(f"✅ {wkn}: {price} €")
                else:
                    print(f"⚠️ {wkn}: Kurs aktuell nur über API/Tabelle sichtbar.")
                sys.stdout.flush()
            except:
                continue
        driver.quit()
    
    print("\n🏁 Mission abgeschlossen. Sentinel im 30-Min-Rhythmus aktiv.")
    sys.stdout.flush()
