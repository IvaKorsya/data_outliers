# detectors/test_detector.py
from core.base_detector import BaseAnomalyDetector
import pandas as pd

class TestDetector(BaseAnomalyDetector):
    def detect(self, data):
        self.results = data.head(10)
        return self.results
        
    def generate_report(self):
        return {
            "summary": "Test report",
            "metrics": {"samples": len(self.results)},
            "tables": {"test_data": self.results},
            "plots": {"test_plot": lambda: plt.plot(self.results.iloc[:, 0])}
        }
