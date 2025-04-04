import yaml
from pathlib import Path

class ConfigManager:
    _instance = None
    
    def __init__(self, config_path='configs/default.yaml'):
        self.config = self._load_config(config_path)
        self._init_colab()
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def _load_config(self, path):
        try:
            with open(Path(__file__).parent.parent / path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return {}
    
    def _init_colab(self):
        if self.config.get('data_loader', {}).get('google_colab', False):
            try:
                from google.colab import drive
                drive.mount('/content/drive')
                print("Google Drive mounted successfully")
            except:
                print("Google Colab not detected, running in local mode")
    
    def get_detector_config(self, detector_name):
        return self.config.get('detectors', {}).get(detector_name, {})
