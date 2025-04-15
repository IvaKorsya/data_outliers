# core/detectors/test_detector.py
from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt

class TestDetector(BaseAnomalyDetector):
    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        print(f"\nTest Detector received data shape: {data.shape}")  
        """Просто возвращаем первые 10 строк для теста"""
        self.results = data.head(10)
        return self.results
        
    def generate_report(self) -> dict:
        return {
            "summary": "Test report - first 10 rows",
            "metrics": {
                "sample_size": len(self.results),
                "columns": list(self.results.columns)
            },
            "tables": {
                "sample_data": self.results
            },
            "plots": {
                "sample_plot": lambda: self.results.plot()
            }
        }
