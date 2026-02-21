import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
from datetime import datetime, timedelta

# --- KONFIGURATION EISERNER STANDARD ---
POOL_FILE = "isin_pool.json"
HERITAGE_DB = "aureum_heritage_db.json"
AUDIT_FILE = "heritage_audit.txt"
ANCHOR_THRESHOLD = 0.0005  # 0,05% Anker
MAX_WORKERS = 12           # Zurück auf 12 parallele Worker

class AureumSentinel:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0'}
        self.load_data()

    def load_data(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = []
        
        if os.path.exists(HERITAGE_DB):
            with open(HERITAGE_DB, "r") as f: self.db = json.load(f)
        else: self.db = {}

    def fetch_asset_data(self, asset):
        """Die 'Marriage' für ein einzelnes Asset (Stooq + Yahoo)."""
        ticker = asset['symbol']
        results = {"ticker": ticker, "status": "failed"}
        
        try:
            # 1. Stooq (Jahrzehnte Historie)
            stooq_sym = ticker.split('.')[0].lower()
            stooq_url = f"https://stooq.com/q/d/l/?s={stooq_sym}&i=d"
            stooq_res = requests.get(stooq_url, headers=self.headers, timeout=10)
            
            # 2. Yahoo (Letzte Woche 5m/Daily)
            yahoo_data = yf.Ticker(ticker).history(period="7d")
            
            if not yahoo_data.empty:
                current_price = yahoo_data['Close'].iloc[-1]
                vola = yahoo_data['Close'].pct_change().std() * (252**0.5)
                
                results = {
                    "ticker": ticker,
                    "price": round(current_price, 4),
                    "vola": round(vola, 4),
                    "stooq_ok": len(stooq_res.content) > 100,
                    "status": "success",
                    "timestamp": datetime.now().isoformat()
                }
        except Exception as e:
            results["error"] = str(e)
            
        return results

    def run_parallel_sync(self):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Pulse-Check mit 12 Workern...")
        
        # Wir nehmen die nächsten 120 Assets aus dem Pool zur Bearbeitung
        work_queue = self.pool[:120] 
        final_reports = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_ticker = {executor.submit(self.fetch_asset_data, a): a for a in work_queue}
            
            for future in concurrent.futures.as_completed(future_to_ticker):
                res = future.result()
                if res["status"] == "success":
                    msg = f"{res['ticker']}: {res['price']} EUR | Vola: {res['vola']} | Stooq: {'✅' if res['stooq_ok'] else '❌'}"
                    final_reports.append(msg)
                    print(f"[AUDIT] {msg}")

        self.generate_audit_report(final_reports)

    def generate_audit_report(self, reports):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write(f"=== AUREUM SENTINEL V188 | PARALLEL HERITAGE [{ts}] ===\n")
            f.write(f"Worker-Threads: {MAX_WORKERS} | Abdeckung: Stooq & Yahoo Hybrid\n")
            f.write("-" * 60 + "\n")
            if reports:
                f.write("\n".join(reports))
            else:
                f.write("Keine Daten extrahiert. Prüfe Internetverbindung/Ticker-Validität.")
            f.write("\n" + "-" * 60 + "\n")

if __name__ == "__main__":
    AureumSentinel().run_parallel_sync()
