import sys
import time  # <--- Das hat gefehlt!
import re
import requests
import os
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- 1. ASSET-DEFINITION ---
CORE_WKNS = ["ENER61", "SAP000", "A1EWWW", "A0AE1X"] 
MARKET_POOL = [
    "BASF11", "DTE000", "VOW300", "ADS000", "DBK100", 
    "ALV001", "BAY001", "BMW111", "IFX000", "MUV200"
]

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    prefs = {"profile.managed_default_content_settings.images": 2, 
             "profile.default_content_settings.stylesheets": 2}
    options.add_experimental_option("prefs", prefs)
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def scan_batch(driver, wkn_list):
    results = []
    history_data = []
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for idx, wkn in enumerate(wkn_list):
        if idx > 0: driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[idx])
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
    
    time.sleep(7.0)

    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        html = driver.page_source
        url = driver.current_url
        wkn_label = re.search(r'aktie/([^/?#]+)', url).group(1) if "aktie/" in url else "Unknown"
        
        bid = re.search(r'id="push-bid".*?>([\d,.]+)<', html)
        ask = re.search(r'id="push-ask".*?>([\d,.]+)<', html)
        news = re.findall(r'class="news-teaser-headline".*?>(.*?)<', html)
        
        b_val = bid.group(1) if bid else "-"
        a_val = ask.group(1) if ask else "-"
        n_val = news[0][:100] if news else "-"
        
        results.append(f"📦 {wkn_label} | B: {b_val} | A: {a_val}")
        history_data.append([ts, wkn_label, b_val, a_val, n_val])

    while len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1]); driver.close()
    driver.switch_to.window(driver.window_handles[0])
    return results, history_data

if __name__ == "__main__":
    start_time = time.time()
    driver = setup_driver()
    all_results = []
    all_history = []
    
    try:
        # Core Assets
        res, hist = scan_batch(driver, CORE_WKNS)
        all_results.extend(res)
        all_history.extend(hist)
        
        # Markt-Pool
        res, hist = scan_batch(driver, MARKET_POOL)
        all_results.extend(res)
        all_history.extend(hist)
    finally:
        driver.quit()

    # CSV HISTORIE SCHREIBEN (Append)
    csv_file = 'sentinel_history.csv'
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['Timestamp', 'WKN', 'Bid', 'Ask', 'News'])
        writer.writerows(all_history)

    # AKTUELLER STAND TXT (Overwrite)
    with open("sentinel_data.txt", "w", encoding="utf-8") as f:
        f.write(f"Zuletzt aktualisiert: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n".join(all_results))
    
    print(f"✅ V75 erfolgreich in {round(time.time()-start_time, 1)}s")
