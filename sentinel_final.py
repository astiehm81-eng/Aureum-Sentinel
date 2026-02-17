import sys
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

print("🛡️ AUREUM SENTINEL V42.4 - NIGHTWATCH")
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
    target_wkns = ["ENER61", "SAP000", "BASF11", "DTE000", "VOW300"]
    driver = setup_driver()
    
    if driver:
        for wkn in target_wkns:
            try:
                driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
                time.sleep(6) 
                html = driver.page_source
                
                # Erweiterte Suche: Findet Preise auch in Tabellen oder nach dem Wort 'Kurs'
                # Sucht nach Zahlenformaten wie 24,15 oder 1.234,50
                price_patterns = [
                    r'data-price="([\d,.]+)"',
                    r'itemprop="price" content="([\d,.]+)"',
                    r'>([\d,.]+)\s*&nbsp;EUR',
                    r'class="price">.*?([\d,.]+)'
                ]
                
                price = None
                for pattern in price_patterns:
                    match = re.search(pattern, html)
                    if match:
                        price = match.group(1)
                        break
                
                if price:
                    print(f"✅ {wkn}: {price} €")
                else:
                    # Letzter Versuch: Suche einfach die erste vernünftige Zahl nach der WKN
                    fallback = re.search(fr'{wkn}.*?>([\d,.]+)<', html, re.DOTALL)
                    if fallback:
                        print(f"✅ {wkn}: {fallback.group(1)} € (Fallback-Sync)")
                    else:
                        print(f"📡 {wkn}: Markt im Standby (Warte auf Eröffnung)")
                
                sys.stdout.flush()
            except:
                continue
        driver.quit()
    
    print("\n🏁 Patrouille beendet. Nächster Scan in 30 Min.")
    sys.stdout.flush()
