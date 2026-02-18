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

TARGET_WKNS = [
    "ENER61", "SAP000", "BASF11", "DTE000", "VOW300", 
    "ADS000", "DBK100", "ALV001", "BAY001", "BMW111",
    "IFX000", "MUV200", "SIE000"
]

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    prefs = {"profile.managed_default_content_settings.images": 2, "profile.default_content_settings.stylesheets": 2}
    options.add_experimental_option("prefs", prefs)
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except: return None

def scan_decagon_v70():
    results = []
    driver = setup_driver()
    if not driver: return ["❌ Browser-Fehler"]

    # --- ERHÖHUNG AUF 10 WORKER ---
    batch_size = 10 
    for i in range(0, len(TARGET_WKNS), batch_size):
        batch = TARGET_WKNS[i:i+batch_size]
        
        # Phase 1: Tabs & 1ms Takt
        for idx, wkn in enumerate(batch):
            if idx > 0: driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[idx])
            driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
            time.sleep(0.001)

        # Synchronisations-Pause für 10 parallele Tabs
        time.sleep(6.0) 

        # Phase 2: Extraktion
        for handle in driver.window_handles:
            driver.switch_to.window(handle)
            html = driver.page_source
            wkn_match = re.search(r'aktie/([^/?#]+)', driver.current_url)
            wkn_label = wkn_match.group(1) if wkn_match else "Unknown"
            
            bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
            ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
            
            if bid and ask:
                b_str = bid.group(1).replace('.', '').replace(',', '.')
                a_str = ask.group(1).replace('.', '').replace(',', '.')
                results.append(f"✅ *{wkn_label}* | B: {b_str} | A: {a_str}")
            else:
                results.append(f"📡 *{wkn_label}* | Standby")

        # Cleanup Tabs für den nächsten Batch (falls vorhanden)
        while len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
            driver.close()
        driver.switch_to.window(driver.window_handles[0])

    driver.quit()
    return results

if __name__ == "__main__":
    print(f"🛡️ AUREUM SENTINEL V70 - DECAGON MODE (10 TABS)")
    sys.stdout.flush()
    start_time = time.time()
    final_results = scan_decagon_v70()
    
    duration = round(time.time() - start_time, 1)
    summary = f"🛰️ *Sentinel Scan Report (V70-Decagon)*\n⏱️ Dauer: {duration}s\n---\n"
    summary += "\n".join(final_results)
    
    print(summary)
    if TELEGRAM_TOKEN != "DEIN_API_TOKEN":
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", 
                      json={"chat_id": CHAT_ID, "text": summary, "parse_mode": "Markdown"})
