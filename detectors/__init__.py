from typing import List, Dict
import pandas as pd
import argparse
from pathlib import Path
from core.runner import AnalysisRunner
from core.config_manager import ConfigManager

class AnomalyDetectionFramework:
    def __init__(self):
        self.detectors = {}
    
    def register_detector(self, name: str, detector_class):
        """Регистрация доступных детекторов"""
        self.detectors[name] = detector_class
    
    def run_pipeline(self, data_path: str, detectors: List[str], 
                    configs: Dict[str, dict] = None):
        """Запуск нескольких детекторов на данных"""
        # Загрузка данных
        data = self._load_data(data_path)
        
        # Применение детекторов
        for det_name in detectors:
            if det_name not in self.detectors:
                print(f"Warning: Detector {det_name} not found")
                continue
            
            print(f"\nRunning {det_name} detector...")
            detector = self.detectors[det_name](configs.get(det_name, {}))
            detector.detect(data)
            report = detector.generate_report()
            
            ReportGenerator.generate(det_name, report)
    
    def _load_data(self, path: str) -> pd.DataFrame:
        """Определяет тип пути и загружает данные"""
        if os.path.isdir(path):
            return DataLoader.load_from_folder(path)
        return DataLoader.load_single_file(path)
