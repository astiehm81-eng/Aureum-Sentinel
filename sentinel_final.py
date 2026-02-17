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
# Trage hier deine Daten ein
TELEGRAM_TOKEN = "DEIN_API_TOKEN"
CHAT_ID = "DEINE_CHAT_ID"

# --- ASSET LISTE (Eiserner Standard) ---
TARGET_WKNS = [
    "ENER61", "SAP000", "BASF11", "DTE000", "VOW300", 
    "ADS000", "DBK100", "ALV001", "BAY001", "BMW111"
]

def send_telegram(message):
    if TELEGRAM_TOKEN == "DEIN_API_TOKEN":
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=10)
    except:
        pass

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except:
        return None

def scan_asset(wkn):
    # Präzisions-Delay 1ms für Schwarm-Sync (wie in Colab)
    time.sleep(0.001)
    driver = setup_driver()
    if not driver:
        return f"❌ {wkn}: Browser-Fehler"
    try:
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
        time.sleep(4)
        html = driver.page_source
        
        # Bid/Ask Suche
        bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
        ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
        
        # Chart-Historie (Buttons 1T, 1W, 1M durchschalten)
        h_status = []
        for period in ["1T", "1W", "1M"]:
            try:
                btn = driver.find_element("xpath", f"//button[contains(., '{period}')]")
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(1.5)
                h_status.append(f"{period}:OK")
            except:
                h_status.append(f"{period}:-")

        if bid and ask:
            return f"✅ *{wkn}* | B: {bid.group(1)} | A: {ask.group(1)} | Hist: {'/'.join(h_status)}"
        return f"📡 *{wkn}* | Warte auf Marktöffnung | Hist: {'/'.join(h_status)}"
    except:
        return f"❌ *{wkn}* | Timeout"
    finally:
        driver.quit()

if __name__ == "__main__":
    print("🛡️ AUREUM SENTINEL V56 INITIALISIERT")
    sys.stdout.flush()
    start_time = time.time()
    
    # 24 Worker Parallel-Modus
    with ThreadPoolExecutor(max_workers=24) as executor:
        results = list(executor.map(scan_asset, TARGET_WKNS))
    
    summary = f"🛰️ *Sentinel Scan Report*\n⏱️ Dauer: {round(time.time()-start_time,1)}s\n---\n"
    summary += "\n".join(results)
    
    print(summary)
    send_telegram(summary)
    sys.stdout.flush()
