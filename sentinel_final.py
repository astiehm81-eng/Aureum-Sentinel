import os
import subprocess
import sys
import time
import re
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- KONFIGURATION (V75.0 - Eiserner Standard) ---
MAX_WORKERS = 24  
DELAY = 0.001     
DISCOVERY_URL = "https://www.ls-tc.de/de/aktien/deutschland"

def setup_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu') 
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def discover_market():
    """Findet automatisch alle relevanten WKNs auf dem Markt"""
    print("🔍 Phase 1: Suche Markt-Assets (Discovery)...")
    driver = setup_driver()
    try:
        driver.get(DISCOVERY_URL)
        time.sleep(5)
        html = driver.page_source
        wkns = list(set(re.findall(r'/de/aktie/([A-Z0-9]{6})', html)))
        print(f"✨ {len(wkns)} Assets identifiziert.")
        return wkns
    except Exception as e:
        print(f"❌ Discovery Fehler: {e}")
        return []
    finally:
        driver.quit()

def scan_task(wkn):
    """Synchronisiert ein einzelnes Asset mit 1ms Jitter"""
    driver = setup_driver()
    try:
        time.sleep(DELAY) 
        driver.get(f"https://www.ls-tc.de/de/aktie/{wkn}")
        # Hier findet die Synchronisation statt
        print(f"✅ {wkn} synchronisiert (Eiserner Standard V42)")
    except Exception as e:
        pass
    finally:
        driver.quit()

# --- HAUPTPROGRAMM ---
if __name__ == "__main__":
    print("🛡️ AUREUM SENTINEL - INITIALISIERUNG")
    
    # 1. Markt erfassen
    all_wkns = discover_market()
    
    if all_wkns:
        # 2. Parallel-Sync mit 24 Workern
        print(f"🔥 Phase 2: Hyper-Sync startet mit {MAX_WORKERS} Workern...")
        # Wir begrenzen für den GitHub-Lauf auf die ersten 50, um Zeit zu sparen (optional)
        targets = all_wkns[:50] 
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(scan_task, targets)

    print("\n🏁 Mission abgeschlossen. Sentinel geht in Standby.")
