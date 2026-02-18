import sys
import time
import re
import random
import requests
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- KONFIGURATION (TELEGRAM WEBHOOK) ---
TELEGRAM_TOKEN = "DEIN_API_TOKEN"
CHAT_ID = "DEINE_CHAT_ID"

# --- ASSET LISTE (Eiserner Standard) ---
TARGET_WKNS = [
    "ENER61", "SAP000", "BASF11", "DTE000", "VOW300", 
    "ADS000", "DBK100", "ALV001", "BAY001", "BMW111"
]

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(25)
        return driver
    except:
        return None

def scan_asset(wkn):
    # --- 1ms PRÄZISIONS-TAKT (Schwarm-Sync) ---
    time.sleep(0.001)
    
    driver = setup_driver()
    if not driver:
        return f"❌ {wkn}: RAM-Limit/Fehler"
    
    try:
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
        # Kurze Lese-Simulation (Optimiert auf 8 Worker)
        time.sleep(3) 
        
        html = driver.page_source
        bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
        ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
        
        # Zeitreihen-Erfassung (Historie-Buttons 1T, 1W, 1M)
        h_status = []
        for period in ["1T", "1W", "1M"]:
            try:
                btn = driver.find_element("xpath", f"//button[contains(., '{period}')]")
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(1.0) # Stabiler Render-Takt
                h_status.append(f"{period}:OK")
            except:
                h_status.append(f"{period}:-")

        if bid and ask:
            return f"✅ *{wkn}* | B: {bid.group(1)} | A: {ask.group(1)} | Hist: {'/'.join(h_status)}"
        return f"📡 *{wkn}* | Marktdaten-Standby | Hist: {'/'.join(h_status)}"
            
    except Exception as e:
        return f"⚠️ *{wkn}* | Timeout ({str(e)[:15]})"
    finally:
        driver.quit()

if __name__ == "__main__":
    print(f"🛡️ AUREUM SENTINEL V61 - 8-WORKER STABILITY MODE")
    sys.stdout.flush()
    
    start_time = time.time()
    
    # --- PARALLEL-MODUS: REDUZIERT AUF 8 WORKER ---
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(scan_asset, TARGET_WKNS))
    
    # Zusammenfassung & Telegram
    duration = round(time.time() - start_time, 1)
    summary = f"🛰️ *Sentinel Scan Report (8-Worker)*\n⏱️ Dauer: {duration}s\n---\n"
    summary += "\n".join(results)
    
    print(summary)
    
    if TELEGRAM_TOKEN != "DEIN_API_TOKEN":
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try:
            requests.post(url, json={"chat_id": CHAT_ID, "text": summary, "parse_mode": "Markdown"}, timeout=10)
        except:
            pass
            
    sys.stdout.flush()
