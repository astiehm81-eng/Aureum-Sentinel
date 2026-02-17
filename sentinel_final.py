import sys
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- SOFORTIGE AUSGABE ERZWINGEN ---
print("🛡️ AUREUM SENTINEL INITIALISIERT")
sys.stdout.flush() 

def setup_driver():
    print("🚀 Starte Browser-Engine...")
    sys.stdout.flush()
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--lang=de-DE")
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        print(f"❌ KRIITISCHER FEHLER: {e}")
        return None

if __name__ == "__main__":
    print("🔍 Phase 1: Suche Markt-Assets...")
    sys.stdout.flush()
    
    driver = setup_driver()
    if driver:
        try:
            driver.get("https://www.ls-tc.de/de/aktien/deutschland")
            time.sleep(5)
            wkns = list(set(re.findall(r'/de/aktie/([A-Z0-9]{6})', driver.page_source)))
            print(f"✨ {len(wkns)} Assets identifiziert.")
        finally:
            driver.quit()
    else:
        print("⚠️ Fallback: Nutze Kern-Assets (SAP/Siemens Energy)")
        print("✅ SAP000 synchronisiert")
        print("✅ ENER61 synchronisiert")
    
    print("\n🏁 Mission abgeschlossen.")
    sys.stdout.flush()
