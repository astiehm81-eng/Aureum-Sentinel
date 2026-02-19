import os, time, random, re, pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright
import pytesseract
from PIL import Image

class AureumSentinelOCR:
    def __init__(self):
        self.csv_path = "ls_prices.csv"
        self.progress_file = "ocr_progress.txt"
        self.batch_size = 30  # Sehr kleiner Batch, um unter dem L&S Radar zu bleiben
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

    def get_universe(self):
        # Hier würden später deine 10.000 ISINs geladen werden
        # Für das Beispiel holen wir eine Referenzliste
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            return sorted(list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))))
        except: return []

    def get_resume_index(self):
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r") as f:
                return int(f.read().strip())
        return 0

    def save_resume_index(self, index):
        with open(self.progress_file, "w") as f:
            f.write(str(index))

    def run(self):
        universe = self.get_universe()
        if not universe:
            self.log("Keine ISINs gefunden.")
            return

        start_idx = self.get_resume_index()
        if start_idx >= len(universe):
            self.log("Universum komplett gescannt. Beginne wieder bei 0.")
            start_idx = 0

        end_idx = min(start_idx + self.batch_size, len(universe))
        current_batch = universe[start_idx:end_idx]
        
        self.log(f"Starte OCR-Lauf für ISINs {start_idx} bis {end_idx} (Trade Republic / L&S)")

        with sync_playwright() as p:
            # Absolute Basis-Tarnung wie im ersten Goldstandard
            browser = p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0"
            )
            page = context.new_page()

            for isin in current_batch:
                try:
                    self.log(f"Optische Erfassung: {isin}")
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", wait_until="domcontentloaded")
                    
                    # Die bewährte Wartezeit aus dem Goldstandard
                    time.sleep(random.uniform(5.0, 7.0))
                    
                    selector = ".price-container"
                    if page.locator(selector).first.is_visible():
                        img_path = f"snap_ls_{isin}.png"
                        page.locator(selector).first.screenshot(path=img_path)
                        
                        # OCR - Nur Zahlen und Trenner
                        config = '--psm 7 -c tessedit_char_whitelist=0123456789,.'
                        text = pytesseract.image_to_string(Image.open(img_path), config=config)
                        
                        price_match = re.search(r'(\d+[\.,]\d+)', text)
                        if price_match:
                            price = float(price_match.group(1).replace(',', '.'))
                            self._save(isin, price)
                            self.log(f"ERFOLG: {isin} -> {price} €")
                        else:
                            self.log(f"OCR konnte Bild nicht entziffern bei {isin}")
                        
                        # Bild nach Analyse löschen um Speicher zu sparen
                        if os.path.exists(img_path): os.remove(img_path)
                    else:
                        self.log(f"Preis-Container nicht sichtbar bei {isin}")

                except Exception as e:
                    self.log(f"FEHLER beim Laden von {isin}")
                
                # WICHTIG: Menschliche Pause zwischen den Aufrufen, damit die L&S Firewall uns in Ruhe lässt
                time.sleep(random.uniform(8.0, 14.0))

            browser.close()
        
        # Fortschritt für den nächsten GitHub-Action Lauf speichern
        self.save_resume_index(end_idx)
        self.log("Batch abgeschlossen. Pausiere bis zum nächsten Trigger.")

    def _save(self, isin, price):
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 
            'Price': price, 
            'Source': 'Trade_Republic_OCR'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

if __name__ == "__main__":
    AureumSentinelOCR().run()
