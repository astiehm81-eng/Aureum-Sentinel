import pyautogui
import pytesseract
import csv
import time
import os
from datetime import datetime

# PFAD ZU TESSERACT (Muss gesetzt sein, z.B. C:\Program Files\Tesseract-OCR\tesseract.exe)
# pytesseract.pytesseract.tesseract_cmd = r'PATH_TO_YOUR_TESSERACT'

CSV_FILE = "sentinel_market_data.csv"

# Definiere hier die Pixel-Bereiche (x, y, breite, höhe) für deine 8-10 Worker-Fenster
# Du musst diese Werte einmal an dein Monitor-Setup anpassen!
WORKER_ZONES = {
    "Siemens_Energy_Ask": (100, 200, 120, 40),
    "Gold_A1KWPQ_Ask":    (300, 200, 120, 40),
    "Nasdaq_Ask":         (500, 200, 120, 40),
    "SAP_Ask":            (700, 200, 120, 40)
}

def initialize_csv():
    if os.path.exists(CSV_FILE):
        os.remove(CSV_FILE)
    with open(CSV_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "Asset", "Price_Ask", "Source"])
    print(f"[*] {CSV_FILE} gelöscht und neu gestartet.")

def run_ocr_sentinel():
    initialize_csv()
    print("[!] OCR-Logger aktiv. Lese Bildschirminhalte...")
    
    while True:
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        
        with open(CSV_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            for name, zone in WORKER_ZONES.items():
                # Screenshot der Zone
                img = pyautogui.screenshot(region=zone)
                # OCR Texterkennung (Nur Zahlen und Kommata)
                raw_text = pytesseract.image_to_string(img, config='--psm 7 -c tessedit_char_whitelist=0123456789,.')
                price = raw_text.strip().replace(',', '.')
                
                if price:
                    writer.writerow([timestamp, name, price, "OCR_Live"])
                    print(f"[{timestamp}] {name}: {price} €") # Sofort-Feedback
                else:
                    print(f"[{timestamp}] {name}: KEIN WERT GEFUNDEN (Prüfe Zone!)")
        
        # High-Speed Intervall
        time.sleep(0.1) 

if __name__ == "__main__":
    run_ocr_sentinel()
