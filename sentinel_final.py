import sys
import time
import re
import requests
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- 1. ASSET-DEFINITION ---
CORE_WKNS = ["ENER61", "SAP000", "A1EWWW", "A0AE1X"] # Siemens Energy, SAP, Gold, Nasdaq
MARKET_POOL = [
    "BASF11", "DTE000", "VOW300", "ADS000", "DBK100", 
    "ALV001", "BAY001", "BMW111", "IFX000", "MUV200",
    "SIE000", "AIR001", "CON000", "RWE000", "TKA000"
]

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    # RAM-Schutz: Deaktiviert Bilder und CSS
    prefs = {"profile.managed_default_content_settings.images": 2, 
             "profile.default_content_settings.stylesheets": 2}
    options.add_experimental_option("prefs", prefs)
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def scan_batch(driver, wkn_list):
    results = []
    # 10er Batches haben sich als stabil erwiesen
    for idx, wkn in enumerate(wkn_list):
        if idx > 0: driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[idx])
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
    
    time.sleep(7.0) # Wartezeit für Push-Daten

    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        html = driver.page_source
        url = driver.current_url
        wkn_label = re.search(r'aktie/([^/?#]+)', url).group(1) if "aktie/" in url else "Unknown"
        
        bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
        ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
        news = re.findall(r'class="news-teaser-headline".*?>(.*?)<', html)
        
        entry = f"📦 {wkn_label} | B: {bid.group(1) if bid else '-'} | A: {ask.group(1) if ask else '-'}"
        if news: entry += f" | NEWS: {news[0][:50]}..."
        results.append(entry)

    # Tabs aufräumen
    while len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1]); driver.close()
    driver.switch_to.window(driver.window_handles[0])
    return results

if __name__ == "__main__":
    start_time = time.time()
    driver = setup_driver()
    all_results = []
    
    try:
        # 1. Core Assets
        all_results.extend(scan_batch(driver, CORE_WKNS))
        # 2. Markt-Pool (In 10er Blöcken)
        for i in range(0, len(MARKET_POOL), 10):
            all_results.extend(scan_batch(driver, MARKET_POOL[i:i+10]))
    finally:
        driver.quit()

    # In Datei schreiben (überschreiben)
    with open("sentinel_data.txt", "w", encoding="utf-8") as f:
        f.write(f"Zuletzt aktualisiert: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n".join(all_results))
    
    print(f"✅ Scan beendet in {round(time.time()-start_time, 1)}s")
