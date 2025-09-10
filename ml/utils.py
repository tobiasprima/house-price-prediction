import yaml
from sqlalchemy import create_engine

def load_config(path="ml/config.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def save_config(config, path="ml/config.yml"):
    with open(path, "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)

def get_engine(db_url: str):
    return create_engine(db_url)
