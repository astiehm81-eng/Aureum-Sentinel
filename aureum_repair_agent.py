import json, os

class GeminiRepairAgent:
    def __init__(self, pool_file="isin_pool.json"):
        self.pool_file = pool_file

    def heal_pool(self, broken_symbol, correct_isin):
        """
        Logik: Ersetzt defekte Ticker durch verifizierte Gemini-Daten.
        """
        if not os.path.exists(self.pool_file):
            data = []
        else:
            with open(self.pool_file, "r") as f: data = json.load(f)
        
        # Symbol-Update
        updated = False
        for asset in data:
            if asset['symbol'] == broken_symbol:
                asset['symbol'] = correct_isin
                updated = True
        
        if not updated:
            data.append({"symbol": correct_isin})
            
        with open(self.pool_file, "w") as f:
            json.dump(data, f, indent=4)
        print(f"✅ Gemini hat {broken_symbol} zu {correct_isin} geheilt.")

# Beispielaufruf (Manuell oder via Engine)
if __name__ == "__main__":
    agent = GeminiRepairAgent()
    # Hier kannst du Symbole manuell heilen, die Yahoo nicht frisst
    # agent.heal_pool("DEF_TICKER", "CORRECT_TICKER.DE")
