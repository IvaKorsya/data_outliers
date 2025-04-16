# core/detectors/test_detector.py
from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt
import json
import logging

class TestDetector(BaseAnomalyDetector):
    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f"Received data shape: {data.shape}")
        self.results = data.head(10)
        return self.results
        
    def generate_report(self) -> dict:
        report = {
            "summary": f"Test report - first {len(self.results)} rows",
            "metrics": {
                "sample_size": len(self.results),
                "columns": list(self.results.columns)
            },
            "tables": {
                "sample_data": self.results
            }
        }
        self.logger.debug(f"Generated report: {report['summary']}")
        return report
            }
        }
