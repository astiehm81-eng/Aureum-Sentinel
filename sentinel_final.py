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

def send_telegram(message):
    if TELEGRAM_TOKEN == "DEIN_API_TOKEN": return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try: requests.post(url, json={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=10)
    except: pass

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu') # Spart RAM
    options.add_argument('--memory-pressure-off') # Ignoriert leichten RAM-Druck
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except: return None

def scan_all_serially():
    results = []
    driver = setup_driver()
    if not driver:
        return ["❌ Browser-Initialisierung fehlgeschlagen"]

    for wkn in TARGET_WKNS:
        # --- 1ms PRÄZISIONS-TAKT ---
        time.sleep(0.001)
        
        try:
            driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
            time.sleep(2) # Kurzer Load reicht seriell aus
            
            html = driver.page_source
            bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
            ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
            
            h_status = []
            # Wir prüfen nur 1T (Tag), um Zeit und RAM zu sparen, 
            # 1W/1M können wir bei Bedarf zuschalten
            for period in ["1T"]:
                try:
                    btn = driver.find_element("xpath", f"//button[contains(., '{period}')]")
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(0.5)
                    h_status.append(f"{period}:OK")
                except: h_status.append(f"{period}:-")

            if bid and ask:
                results.append(f"✅ *{wkn}* | B: {bid.group(1)} | A: {ask.group(1)} | {h_status[0]}")
            else:
                results.append(f"📡 *{wkn}* | Standby | {h_status[0]}")
                
        except:
            results.append(f"⚠️ *{wkn}* | Error")
            # Falls ein Fehler auftritt, Driver kurz neu starten
            driver.quit()
            driver = setup_driver()
            
    driver.quit()
    return results

if __name__ == "__main__":
    print(f"🛡️ AUREUM SENTINEL V63 - STABILITY FOCUS")
    sys.stdout.flush()
    start_time = time.time()
    
    final_results = scan_all_serially()
    
    duration = round(time.time() - start_time, 1)
    summary = f"🛰️ *Sentinel Scan Report (Stable Mode)*\n⏱️ Dauer: {duration}s\n---\n"
    summary += "\n".join(final_results)
    
    print(summary)
    send_telegram(summary)
    sys.stdout.flush()
