import os, time, random, re, pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright
import pytesseract
from PIL import Image
import sys

class AureumSentinel:
    def __init__(self):
        self.csv_path = "sentinel_history.csv"
        self.progress_file = "sentinel_data.txt"
        # TESTMODUS: 3 Aktien für schnelles Feedback bei der Arbeit
        self.batch_size = 3 
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def log(self, msg):
        # flush=True drückt den Text sofort in die GitHub Action Konsole
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] 🛡️ SENTINEL: {msg}", flush=True)

    def get_universe(self):
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            isins = sorted(list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))))
            return isins
        except: 
            self.log("❌ FEHLER: Universum konnte nicht geladen werden.")
            return []

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
        
        self.log(f"🚀 START TEST-BATCH (Index {start_idx} bis {end_idx})")

        with sync_playwright() as p:
            self.log("🌐 Browser-Engine wird hochgefahren...")
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(viewport={'width': 1280, 'height': 800})
            page = context.new_page()

            for i, isin in enumerate(current_batch):
                self.log(f"🔎 [{i+1}/{self.batch_size}] Scanne ISIN: {isin}")
                try:
                    # Direkter Aufruf L&S / Trade Republic
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", wait_until="domcontentloaded", timeout=40000)
                    
                    # Warten auf Preis-Rendering (Eiserner Standard Stealth Pause)
                    time.sleep(8) 
                    
                    selector = ".price-container"
                    if page.locator(selector).first.is_visible():
                        img_path = f"snap_{isin}.png"
                        page.locator(selector).first.screenshot(path=img_path)
                        
                        # Optische Analyse (OCR)
                        config = '--psm 7 -c tessedit_char_whitelist=0123456789,.'
                        text = pytesseract.image_to_string(Image.open(img_path), config=config, lang='deu')
                        
                        price_match = re.search(r'(\d+[\.,]\d+)', text)
                        if price_match:
                            price = float(price_match.group(1).replace(',', '.'))
                            self._save(isin, price)
                            self.log(f"✅ TREFFER: {isin} bei {price} €")
                        else:
                            self.log(f"⚠️ OCR-LESEN FEHLGESCHLAGEN. Bildinhalt: '{text.strip()}'")
                        
                        if os.path.exists(img_path): os.remove(img_path)
                    else:
                        self.log(f"🚫 BLOCKADE: L&S Preis-Element für {isin} nicht sichtbar.")
                except Exception as e:
                    self.log(f"🕒 TIMEOUT oder Netzwerkfehler bei {isin}")
                
                # Sicherheitsintervall
                time.sleep(5)

            browser.close()
        
        self.save_resume_index(end_idx)
        self.log(f"🏁 BATCH BEENDET. Fortschritt {end_idx} in sentinel_data.txt gespeichert.")

    def _save(self, isin, price):
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 
            'Price': price, 
            'Source': 'L&S_OCR_STEALTH'
        }])
        # Speichert direkt in deine bestehende CSV
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

if __name__ == "__main__":
    AureumSentinel().run()
