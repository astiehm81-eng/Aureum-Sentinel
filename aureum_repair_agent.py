import json
import os
import pandas as pd

# --- KONFIGURATION (ABGESTIMMT AUF V107.8) ---
POOL_FILE = "isin_pool.json"
STATUS_FILE = "vault_status.txt"

class AureumRepairAgent:
    def __init__(self):
        self.pool = self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f:
                return json.load(f)
        return []

    def log_repair(self, msg):
        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(STATUS_FILE, "a") as f:
            f.write(f"[{timestamp}] [REPAIR-AGENT] {msg}\n")
        print(f"🚑 {msg}")

    def update_ticker(self, old_symbol, new_symbol):
        """Ersetzt einen defekten Ticker durch einen von Gemini verifizierten."""
        updated = False
        for asset in self.pool:
            if asset['symbol'] == old_symbol:
                asset['symbol'] = new_symbol
                updated = True
        
        if updated:
            self.save_pool()
            self.log_repair(f"Ticker geheilt: {old_symbol} -> {new_symbol}")
        else:
            self.add_new_asset(new_symbol)

    def add_new_asset(self, symbol):
        """Fügt ein neues Asset hinzu, falls es noch nicht im Pool ist."""
        if not any(a['symbol'] == symbol for a in self.pool):
            self.pool.append({"symbol": symbol})
            self.save_pool()
            self.log_repair(f"Neues Asset in Pool integriert: {symbol}")

    def save_pool(self):
        with open(POOL_FILE, "w") as f:
            json.dump(self.pool, f, indent=4)

# --- ANWENDUNG ---
if __name__ == "__main__":
    agent = AureumRepairAgent()
    
    # Beispiel: Wenn du merkst, dass ein Ticker (z.B. 'SAP') nicht mehr geht,
    # kannst du mich (Gemini) fragen und das Ergebnis hier eintragen:
    # agent.update_ticker("SAP", "SAP.DE")
