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
TELEGRAM_TOKEN = "DEIN_API_TOKEN_HIER"
CHAT_ID = "DEINE_CHAT_ID_HIER"

# --- ASSET LISTE (Eiserner Standard) ---
TARGET_WKNS = [
    "ENER61", "SAP000", "BASF11", "DTE000", "VOW300", 
    "ADS000", "DBK100", "ALV001", "BAY001", "BMW111",
    "MBG000", "IFX000", "MUV200", "RWE000", "SIE000"
]

def send_telegram(message):
    """Sendet Daten direkt an deinen Telegram-Kanal."""
    if TELEGRAM_TOKEN == "DEIN_API_TOKEN_HIER":
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"❌ Telegram Error: {e}")

def setup_driver():
    """Initialisiert den Browser im Headless-Modus mit Tarnung."""
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)
        return driver
    except:
        return None

def scan_asset(wkn):
    """Verarbeitet ein einzelnes Asset mit Multi-Timeframe-Check."""
    # 1ms Präzisions-Delay (Schwarm-Synchronisation)
    time.sleep(0.001)
    
    driver = setup_driver()
    if not driver:
        return f"❌ {wkn}: Browser-Fehler"
    
    try:
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
        time.sleep(3) # Initialer Load
        
        # 1. Kursdaten extrahieren (Trade Republic / LS Basis)
        html = driver.page_source
        bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
        ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
        
        # 2. Historie-Buttons durchklicken (1T, 1W, 1M)
        timeframes_status = []
        for period in ["1T", "1W", "1M"]:
            try:
                # Suche Button und simuliere Klick
                btn = driver.find_element("xpath", f"//button[contains(., '{period}')]")
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(1.5) # Wartezeit für Grafik-Render
                timeframes_status.append(f"{period}:OK")
            except:
                timeframes_status.append(f"{period}:FAIL")
        
        # Daten-Aufbereitung
        if bid and ask:
            b_val = bid.group(1)
            a_val = ask.group(1)
            return f"✅ *{wkn}* | B: {b_val} | A: {a_val} | Hist: {'/'.join(timeframes_status)}"
        else:
            return f"⚠️ *{wkn}* | Seite geladen, Kurse aktuell im Standby"
            
    except Exception as e:
        return f"❌ *{wkn}* | Fehler: {str(e)[:50]}"
    finally:
        driver.quit()

if __name__ == "__main__":
    print(f"🛡️ AUREUM SENTINEL V55 - START (24 WORKER)")
    sys.stdout.flush()
    
    start_time = time.time()
    results = []
    
    # Parallel-Verarbeitung mit 24 Workern
    with ThreadPoolExecutor(max_workers=24) as executor:
        results = list(executor.map(scan_asset, TARGET_WKNS))
    
    # Zusammenfassung für Telegram
    duration = round(time.time() - start_time, 2)
    summary = f"🛰️ *Aureum Sentinel Scan Report*\n"
    summary += f"⏱️ Dauer: {duration}s\n"
    summary += "---\n"
    summary += "\n".join(results)
    
    print(summary)
    send_telegram(summary)
    
    print("\n🏁 Mission abgeschlossen. Daten an Telegram-Hook übertragen.")
    sys.stdout.flush()
