import os
import json

def load_config() -> dict:
    if os.path.exists("config/config.json"):
        path = "config/config.json"
    else:
        path = "config/config_template.json"
    with open(path) as f:
        return json.load(f)

CONFIG = load_config()