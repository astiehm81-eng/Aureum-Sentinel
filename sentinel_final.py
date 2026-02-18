import sys
import time
import re
import requests
from concurrent.futures import ThreadPoolExecutor
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
    
    # --- RADIKALE RAM-SCHONUNG ---
    # 1. Schaltet Bilder aus
    # 2. Deaktiviert CSS (kein Styling)
    # 3. Verhindert das Laden von Plugins
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_settings.stylesheets": 2,
        "profile.default_content_settings.cookies": 2,
        "profile.default_content_settings.javascript": 1, # JS muss für Push-Kurse an bleiben
        "profile.managed_default_content_settings.fonts": 2
    }
    options.add_experimental_option("prefs", prefs)
    
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(20)
        return driver
    except: return None

def scan_asset(wkn):
    time.sleep(0.001) # 1ms Taktung
    driver = setup_driver()
    if not driver: return f"❌ {wkn}: RAM-Limit"
    
    try:
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
        # Da wir kein CSS laden, ist die Seite VIEL schneller bereit
        time.sleep(2.5) 
        
        html = driver.page_source
        bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
        ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
        
        # Hist-Buttons funktionieren oft auch ohne CSS (nur über JS/DOM)
        h_status = "1T:-"
        try:
            btn = driver.find_element("xpath", "//button[contains(., '1T')]")
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.5)
            h_status = "1T:OK"
        except: pass

        if bid and ask:
            return f"✅ *{wkn}* | B: {bid.group(1)} | A: {ask.group(1)} | {h_status}"
        return f"📡 *{wkn}* | Standby | {h_status}"
    except:
        return f"⚠️ *{wkn}* | Timeout"
    finally:
        driver.quit()

if __name__ == "__main__":
    print(f"🛡️ AUREUM SENTINEL V66 - RAM-SAFE DUAL-WORKER")
    sys.stdout.flush()
    start_time = time.time()
    
    # --- TEST MIT 2 WORKERN + RAM-SCHONUNG ---
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(scan_asset, TARGET_WKNS))
    
    duration = round(time.time() - start_time, 1)
    summary = f"🛰️ *Sentinel Scan Report (Dual-Worker Optimized)*\n⏱️ Dauer: {duration}s\n---\n"
    summary += "\n".join(results)
    
    print(summary)
    
    if TELEGRAM_TOKEN != "DEIN_API_TOKEN":
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try: requests.post(url, json={"chat_id": CHAT_ID, "text": summary, "parse_mode": "Markdown"}, timeout=10)
        except: pass
