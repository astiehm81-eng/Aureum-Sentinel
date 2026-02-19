import os, time, random, re, pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright
import pytesseract
from PIL import Image

class AureumSentinel:
    def __init__(self):
        self.csv_path = "sentinel_history.csv"
        self.progress_file = "sentinel_data.txt"
        self.batch_size = 3 
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛡️ [BRIEF-MODUS]: {msg}", flush=True)

    def get_universe(self):
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=10)
            return sorted(list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))))
        except: return []

    def run(self):
        universe = self.get_universe()
        if not universe: return
        
        idx = 0
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r") as f:
                c = f.read().strip()
                idx = int(c) if c.isdigit() else 0
        
        current_batch = universe[idx:idx + self.batch_size]
        self.log(f"Starte optische Erfassung (Index {idx} bis {idx+self.batch_size})")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 800}
            )
            page = context.new_page()

            for isin in current_batch:
                self.log(f"Fokussiere Brief-Wert für {isin}...")
                try:
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", wait_until="networkidle", timeout=60000)
                    time.sleep(7) 

                    # Gezielter Selektor für den Brief-Wert (Ask) Bereich
                    # Wir nehmen den Container, der den Text 'Brief' oder die Quote enthält
                    brief_selector = ".quote-brief, .price-container-ask, .quotedata-ask"
                    
                    target = None
                    for sel in brief_selector.split(','):
                        if page.locator(sel.strip()).first.is_visible():
                            target = page.locator(sel.strip()).first
                            break
                    
                    if not target:
                        # Fallback auf den Hauptcontainer, falls Klassen variieren
                        target = page.locator(".price-container").first

                    if target and target.is_visible():
                        img_path = f"ask_{isin}.png"
                        target.screenshot(path=img_path)
                        
                        # OCR-Konfiguration für Zahlenwerte
                        config = '--psm 7 -c tessedit_char_whitelist=0123456789,.'
                        text = pytesseract.image_to_string(Image.open(img_path), config=config, lang='deu')
                        
                        match = re.search(r'(\d+[\.,]\d+)', text)
                        if match:
                            price = float(match.group(1).replace(',', '.'))
                            self._save(isin, price)
                            self.log(f"✅ BRIEF-WERT ERKANNT: {price} €")
                        else:
                            self.log(f"⚠️ OCR konnte Brief-Wert nicht lesen. Text war: '{text.strip()}'")
                        
                        if os.path.exists(img_path): os.remove(img_path)
                    else:
                        self.log(f"🚫 Element 'Brief' für {isin} nicht sichtbar.")

                except Exception as e:
                    self.log(f"❌ Fehler bei {isin}")
                
                time.sleep(random.uniform(5, 10))

            browser.close()
        
        with open(self.progress_file, "w") as f:
            f.write(str(idx + self.batch_size))

    def _save(self, isin, price):
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            'ISIN': isin, 'Price': price, 'Source': 'L&S_OPTICAL_ASK_V153'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

if __name__ == "__main__":
    AureumSentinel().run()
