from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pandas as pd

class AnalysisRunner:
    def __init__(self):
        self.detectors = {}
        self.config = ConfigManager.get_instance()
    
    def register_detector(self, name: str, detector_class):
        self.detectors[name] = detector_class
    
    def run(self, data_path: str, detectors: list, output_format: str = 'console'):
        """Запуск анализа с поддержкой параллельного выполнения"""
        data = DataLoader().load(data_path)
        results = {}
        
        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    self._run_detector,
                    detector_name,
                    data.copy()
                ): detector_name 
                for detector_name in detectors
                if detector_name in self.detectors
            }
            
            for future in futures:
                detector_name = futures[future]
                try:
                    results[detector_name] = future.result()
                except Exception as e:
                    print(f"Ошибка в {detector_name}: {str(e)}")
        
        self._save_results(results, output_format)
        return results
    
    def _run_detector(self, name: str, data: pd.DataFrame):
        detector = self.detectors[name](self.config.get_detector_config(name))
        detector.detect(data)
        return detector.generate_report()
    
    def _save_results(self, results: dict, format: str):
        ReportGenerator().save(results, format)
