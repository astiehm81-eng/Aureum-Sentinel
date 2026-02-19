import os, time, random, re, pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright
import pytesseract
from PIL import Image

class AureumSentinel:
    def __init__(self):
        # Wir nutzen deine bestehende CSV
        self.csv_path = "sentinel_history.csv"
        # Wir nutzen deine sentinel_data.txt als Fortschrittsspeicher
        self.progress_file = "sentinel_data.txt"
        self.batch_size = 20 
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

    def get_universe(self):
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            return sorted(list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))))
        except: return []

    def get_resume_index(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r") as f:
                    content = f.read().strip()
                    return int(content) if content.isdigit() else 0
            except: return 0
        return 0

    def save_resume_index(self, index):
        with open(self.progress_file, "w") as f:
            f.write(str(index))

    def run(self):
        universe = self.get_universe()
        if not universe: return

        start_idx = self.get_resume_index()
        if start_idx >= len(universe): start_idx = 0
        end_idx = min(start_idx + self.batch_size, len(universe))
        current_batch = universe[start_idx:end_idx]
        
        self.log(f"OCR-Lauf: ISIN {start_idx} bis {end_idx} (L&S / Trade Republic)")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(viewport={'width': 1280, 'height': 800})
            page = context.new_page()

            for isin in current_batch:
                try:
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", wait_until="domcontentloaded", timeout=30000)
                    time.sleep(7) # Zeit zum Laden der Kurse
                    
                    selector = ".price-container"
                    if page.locator(selector).first.is_visible():
                        img_path = f"snap_{isin}.png"
                        page.locator(selector).first.screenshot(path=img_path)
                        
                        config = '--psm 7 -c tessedit_char_whitelist=0123456789,.'
                        text = pytesseract.image_to_string(Image.open(img_path), config=config)
                        price_match = re.search(r'(\d+[\.,]\d+)', text)
                        
                        if price_match:
                            price = float(price_match.group(1).replace(',', '.'))
                            self._save(isin, price)
                            self.log(f"ERFOLG: {isin} -> {price}")
                        
                        if os.path.exists(img_path): os.remove(img_path)
                except: continue
                time.sleep(random.uniform(5, 10))

            browser.close()
        self.save_resume_index(end_idx)

    def _save(self, isin, price):
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': price, 'Source': 'L&S_OCR_V156'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

if __name__ == "__main__":
    AureumSentinel().run()
