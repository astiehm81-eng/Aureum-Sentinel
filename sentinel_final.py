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
    # RAM-Schonung: Keine Bilder, kein CSS
    prefs = {"profile.managed_default_content_settings.images": 2, "profile.default_content_settings.stylesheets": 2}
    options.add_experimental_option("prefs", prefs)
    try:
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    except: return None

def scan_batch():
    results = []
    driver = setup_driver()
    if not driver: return ["❌ Browser-Start fehlgeschlagen"]

    # Wir teilen die Liste in 2er Paare auf (Parallel-Simulation im RAM-Limit)
    for i in range(0, len(TARGET_WKNS), 2):
        batch = TARGET_WKNS[i:i+2]
        # Taktung 1ms
        time.sleep(0.001)
        
        for idx, wkn in enumerate(batch):
            try:
                if idx > 0: # Neuen Tab öffnen für das zweite Asset im Paar
                    driver.execute_script("window.open('');")
                    driver.switch_to.window(driver.window_handles[idx])
                
                driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
            except: results.append(f"⚠️ {wkn}: Load Fail")

        time.sleep(3.5) # Warten bis beide geladen sind

        # Daten aus beiden Tabs extrahieren
        for handle in driver.window_handles:
            driver.switch_to.window(handle)
            wkn_current = re.search(r'aktie/(.*)', driver.current_url).group(1) if "aktie/" in driver.current_url else "Unknown"
            html = driver.page_source
            bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
            ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
            
            status = "✅" if bid else "📡"
            results.append(f"{status} *{wkn_current}* | B: {bid.group(1) if bid else '-'} | A: {ask.group(1) if ask else '-'}")

        # Tabs schließen außer den ersten, um RAM für das nächste Paar frei zu machen
        while len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
            driver.close()
        driver.switch_to.window(driver.window_handles[0])

    driver.quit()
    return results

if __name__ == "__main__":
    print("🛡️ AUREUM SENTINEL V67 - BATCH-TAB MODE (RAM-FIX)")
    start_time = time.time()
    final_results = scan_batch()
    
    summary = f"🛰️ *Sentinel Scan Report (Batch-Mode)*\n⏱️ Dauer: {round(time.time()-start_time,1)}s\n---\n"
    summary += "\n".join(final_results)
    print(summary)
    
    if TELEGRAM_TOKEN != "DEIN_API_TOKEN":
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", 
                      json={"chat_id": CHAT_ID, "text": summary, "parse_mode": "Markdown"})
