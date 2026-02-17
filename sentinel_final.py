import sys
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

print("🛡️ AUREUM SENTINEL V45 - SPREAD-PRECISION")
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
    except: return None

if __name__ == "__main__":
    target_wkns = ["ENER61", "SAP000", "BASF11", "DTE000", "VOW300"]
    driver = setup_driver()
    
    if driver:
        for wkn in target_wkns:
            try:
                driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
                time.sleep(7) 
                html = driver.page_source
                
                # Wir suchen gezielt nach den Bid/Ask Containern aus deinem Screenshot
                # Diese befinden sich oft in "push-bid" und "push-ask" Klassen
                bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
                ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
                
                if bid and ask:
                    b_val = bid.group(1)
                    a_val = ask.group(1)
                    # Berechnung des Spreads in Prozent (für den Eisernen Standard)
                    try:
                        b_float = float(b_val.replace('.', '').replace(',', '.'))
                        a_float = float(a_val.replace('.', '').replace(',', '.'))
                        spread = ((a_float - b_float) / b_float) * 100
                        print(f"✅ {wkn} | Bid: {b_val} | Ask: {a_val} | Spread: {spread:.3f}%")
                    except:
                        print(f"✅ {wkn} | Bid: {b_val} | Ask: {a_val}")
                else:
                    # Fallback Suche für das Nacht-Layout
                    fallback_price = re.search(r'class="price".*?>([\d,.]+)<', html)
                    if fallback_price:
                        print(f"✅ {wkn}: {fallback_price.group(1)} € (Last)")
                    else:
                        print(f"📡 {wkn}: Warte auf Kursdaten...")
                
                sys.stdout.flush()
            except: continue
        driver.quit()
    
    print("\n🏁 Mission abgeschlossen. Monitoring läuft.")
    sys.stdout.flush()
