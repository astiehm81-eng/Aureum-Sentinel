import sys
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

print("🛡️ AUREUM SENTINEL V42.2 - LIVE")
sys.stdout.flush()

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--lang=de-DE")
    # Tarnt den Bot als normalen Desktop-Nutzer
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    try:
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f"❌ Browser-Fehler: {e}")
        return None

if __name__ == "__main__":
    # DEINE PRIORITÄTS-LISTE (Eiserner Standard)
    # Hier kannst du jederzeit weitere WKNs hinzufügen
    target_wkns = ["ENER61", "SAP000", "BASF11", "DTE000", "VOW300"]
    
    print(f"🔍 Starte Überwachung von {len(target_wkns)} Kern-Assets...")
    sys.stdout.flush()
    
    driver = setup_driver()
    if driver:
        for wkn in target_wkns:
            try:
                # Direkter Aufruf der Asset-Seite
                driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
                time.sleep(3) # Kurze Pause für Daten-Sync
                
                # Wir suchen den aktuellen Kurs im HTML
                html = driver.page_source
                # Extrahiert den Preis (sucht nach dem Muster "price": "123,45")
                price_match = re.search(r'data-price="([\d,.]+)"', html)
                
                if price_match:
                    price = price_match.group(1)
                    print(f"✅ {wkn}: {price} € (Trade Republic Sync)")
                else:
                    print(f"📡 {wkn}: Verbindung stabil, Warte auf Kurs...")
                
                sys.stdout.flush()
            except Exception as e:
                print(f"⚠️ Fehler bei {wkn}: {e}")
        
        driver.quit()
    
    print("\n🏁 Patrouille beendet. Sentinel geht in Standby.")
    sys.stdout.flush()
