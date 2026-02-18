import sys
import time
import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- KONFIGURATION ---
TELEGRAM_TOKEN = "DEIN_API_TOKEN"
CHAT_ID = "DEINE_CHAT_ID"
TARGET_WKNS = ["ENER61", "SAP000", "BASF11", "DTE000", "VOW300", "ADS000", "DBK100", "ALV001", "BAY001", "BMW111"]

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    # RAM-Schutz: Deaktiviert Bilder und CSS für High-Speed
    prefs = {"profile.managed_default_content_settings.images": 2, "profile.default_content_settings.stylesheets": 2}
    options.add_experimental_option("prefs", prefs)
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)
        return driver
    except: return None

def scan_pentagon_batch():
    results = []
    driver = setup_driver()
    if not driver: return ["❌ Browser-Start fehlgeschlagen"]

    # 5er Batches für maximale RAM-Effizienz bei hoher Parallelität
    batch_size = 5
    for i in range(0, len(TARGET_WKNS), batch_size):
        batch = TARGET_WKNS[i:i+batch_size]
        
        # Phase 1: Tabs öffnen und URLs laden
        for idx, wkn in enumerate(batch):
            if idx > 0:
                driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[idx])
            driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
            time.sleep(0.001) # 1ms Taktung zwischen den Tab-Befehlen

        # Phase 2: Kurze Synchron-Pause (Warten auf JS-Push)
        time.sleep(4.5) 

        # Phase 3: Daten-Extraktion aus allen offenen Tabs
        for handle in driver.window_handles:
            driver.switch_to.window(handle)
            html = driver.page_source
            
            # WKN aus URL extrahieren für korrekte Zuordnung
            current_url = driver.current_url
            wkn_match = re.search(r'aktie/([^/?#]+)', current_url)
            wkn_label = wkn_match.group(1) if wkn_match else "Unknown"
            
            bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
            ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
            
            status = "✅" if bid else "📡"
            results.append(f"{status} *{wkn_label}* | B: {bid.group(1) if bid else '-'} | A: {ask.group(1) if ask else '-'}")

        # Tabs aufräumen für den nächsten 5er Batch
        while len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
            driver.close()
        driver.switch_to.window(driver.window_handles[0])

    driver.quit()
    return results

if __name__ == "__main__":
    print("🛡️ AUREUM SENTINEL V68 - PENTAGON-TAB MODE")
    sys.stdout.flush()
    start_time = time.time()
    
    final_results = scan_pentagon_batch()
    
    duration = round(time.time() - start_time, 1)
    summary = f"🛰️ *Sentinel Scan Report (Pentagon-5)*\n⏱️ Dauer: {duration}s\n---\n"
    summary += "\n".join(final_results)
    
    print(summary)
    if TELEGRAM_TOKEN != "DEIN_API_TOKEN":
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", 
                      json={"chat_id": CHAT_ID, "text": summary, "parse_mode": "Markdown"})
    sys.stdout.flush()
