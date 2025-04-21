# core/config_manager.py
import yaml
from pathlib import Path
import logging

class ConfigManager:
    _instance = None
    
    def __init__(self, config_path='local.yaml'):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config_dir = Path(__file__).parent.parent / 'core' / 'config'
        self.config_file = Path(config_path)
        self.logger.info(f"Loading config from: {self.config_file}")
        self.config = self._load_config()
    
    def _load_config(self):
        try:
            with open(self.config_file) as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Config loading failed: {str(e)}")
            return {}