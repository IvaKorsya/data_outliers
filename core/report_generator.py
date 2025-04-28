import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json
import os
from pathlib import Path
from typing import Dict
from IPython.display import display

class ReportGenerator:
    def generate(self, detector_name: str, report_data: Dict):
        """Генерация отчета в консоли"""
        print(f"\n{'='*50}")
        print(f"{detector_name.upper()} REPORT")
        print("="*50)
        
        if 'error' in report_data:
            print(f"Error: {report_data['error']}")
            return
        
        # Вывод сводки
        print("\nSUMMARY:")
        print(report_data.get('summary', 'No summary available'))
        
        # Вывод таблиц
        if 'tables' in report_data:
            for name, df in report_data['tables'].items():
                print(f"\n{name}:")
                display(df.head())
        
        # Построение графиков
        if 'plots' in report_data:
            for name, func in report_data['plots'].items():
                print(f"\nPlot: {name}")
                func()
                plt.show()
    
    def save_summary(self, results: Dict, output_dir: str, format: str = 'json'):
        """Сохранение сводного отчета"""
        summary = {
            'successful': [name for name, res in results.items() if 'error' not in res],
            'failed': [name for name, res in results.items() if 'error' in res],
            'summary_metrics': self._collect_metrics(results)
        }
        
        Path(output_dir).mkdir(exist_ok=True)
        
        if format == 'json':
            with open(Path(output_dir) / 'summary_report.json', 'w') as f:
                json.dump(summary, f, indent=2, default=self._convert_np)
        elif format == 'html':
            self._generate_html(summary, output_dir)

    def _collect_metrics(self, results: Dict) -> Dict:
        """Сбор метрик из всех детекторов"""
        metrics = {}
        for name, res in results.items():
            if 'metrics' in res:
                metrics[name] = res['metrics']
        return metrics

    def _convert_np(self, obj):
        """Корректное преобразование типов numpy для сериализации"""
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        return str(obj)  # На всякий случай

    def _generate_html(self, summary, output_dir):
        """Генерация HTML-отчета"""
        num_detectors_run = len(summary.get('successful', [])) + len(summary.get('failed', []))
        
        html = f"""
        <html>
        <head><title>Anomaly Detection Report</title></head>
        <body>
            <h1>Anomaly Detection Summary</h1>
            <p>Detectors run: {num_detectors_run}</p>
            <h2>Results</h2>
            <ul>
                <li>Successful: {', '.join(summary.get('successful', []))}</li>
                <li>Failed: {len(summary.get('failed', []))}</li>
            </ul>
            <h2>Metrics</h2>
            <pre>{json.dumps(summary['summary_metrics'], indent=2, default=self._convert_np)}</pre>
        </body>
        </html>
        """

        output_path = os.path.join(output_dir, "summary.html")
        with open(output_path, "w") as f:
            f.write(html)
