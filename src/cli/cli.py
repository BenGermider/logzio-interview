import json
from pathlib import Path

# Current file directory
CURRENT_DIR = Path(__file__).parent

# Config file relative to this CLI script
CONFIG_FILE = CURRENT_DIR.parent / "pipeline" / "config.json"

def load_config():
    if Path(CONFIG_FILE).exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_config(config):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)

def update_cities(config, source_index, add_city=None, remove_city=None):
    changed = False

    if source_index < 0 or source_index >= len(config):
        print(f"Invalid source index: {source_index}")
        return config, changed

    source = config[source_index]

    if "cities" not in source:
        source["cities"] = []

    if add_city and add_city not in source["cities"]:
        source["cities"].append(add_city)
        changed = True

    if remove_city and remove_city in source["cities"]:
        source["cities"].remove(remove_city)
        changed = True

    return config, changed

if __name__ == "__main__":
    print("Interactive CLI running. Type commands like:")
    print("  add Berlin 0")
    print("  remove Paris 1")
    print("  show")
    print("  exit")

    config = load_config()

    while True:
        try:
            cmd = input("> ").strip()
            if cmd.lower() == "exit":
                break
            if cmd.lower() == "show":
                print(json.dumps(config, indent=2))
                continue

            parts = cmd.split()
            if len(parts) != 3:
                print("Invalid command format. Example: add Berlin 0")
                continue

            action, city, index_str = parts
            try:
                index = int(index_str)
            except ValueError:
                print("Source index must be an integer")
                continue

            if action.lower() == "add":
                config, changed = update_cities(config, index, add_city=city)
            elif action.lower() == "remove":
                config, changed = update_cities(config, index, remove_city=city)
            else:
                print("Unknown action. Use 'add' or 'remove'.")
                continue

            if changed:
                save_config(config)
                print("Config updated.")
            else:
                print("No change.")

        except KeyboardInterrupt:
            print("\nExiting CLI...")
            break
