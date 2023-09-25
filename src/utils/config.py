import os

from pathlib import Path
import yaml


config_path = os.path.join(Path(__file__).parents[1], 'config.yaml')

# with open('../config.yaml', 'r') as f:
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)


def get_config():
    return config

