
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List
import pandas as pd
import os
from pathlib import Path

class AnalysisRunner:
    def __init__(self, config: Dict = None):
        self.detectors = {}
        self.config = config or {}
        self.data_loader = DataLoader(config.get('data_loader', {}))
        self.report_generator = ReportGenerator()
    
    def register_detector(self, name: str, detector_class):
        """Регистрация детектора с валидацией"""
        if not hasattr(detector_class, 'detect') or not hasattr(detector_class, 'generate_report'):
            raise ValueError(f"Detector {name} must implement required methods")
        self.detectors[name] = detector_class
    
    def run(self, data_path: str, detectors: List[str], 
            output_format: str = 'console', output_dir: str = 'reports') -> Dict:
        """Запуск анализа с поддержкой параллельного выполнения"""
        # Загрузка данных
        try:
            data = self.data_loader.load(data_path)
        except Exception as e:
            raise ValueError(f"Data loading failed: {str(e)}")
        
        # Фильтрация доступных детекторов
        valid_detectors = [d for d in detectors if d in self.detectors]
        if not valid_detectors:
            raise ValueError("No valid detectors specified")
        
        # Параллельное выполнение
        results = {}
        with ThreadPoolExecutor(max_workers=min(4, len(valid_detectors))) as executor:
            futures = {
                executor.submit(
                    self._run_single_detector,
                    name,
                    data.copy()
                ): name for name in valid_detectors
            }
            
            for future in futures:
                name = futures[future]
                try:
                    results[name] = future.result()
                    self._save_detector_output(name, results[name], output_dir)
                except Exception as e:
                    results[name] = {'error': str(e)}
                    print(f"Detector {name} failed: {str(e)}")
        
        # Генерация отчетов
        if output_format != 'console':
            Path(output_dir).mkdir(exist_ok=True)
            self.report_generator.save_summary(results, output_dir, output_format)
        
        return results
    
    def _run_single_detector(self, name: str, data: pd.DataFrame) -> Dict:
        """Запуск одного детектора"""
        detector = self.detectors[name](self.config.get('detectors', {}).get(name, {}))
        detector.detect(data)
        return detector.generate_report()
    
    def _save_detector_output(self, name: str, report: Dict, output_dir: str):
        """Сохранение результатов детектора"""
        detector_dir = Path(output_dir) / name
        detector_dir.mkdir(parents=True, exist_ok=True)
        
        if 'error' in report:
            return
            
        # Сохранение JSON
        with open(detector_dir / 'report.json', 'w') as f:
            json.dump({
                'summary': report.get('summary'),
                'metrics': report.get('metrics', {})
            }, f, indent=2)
        
        # Сохранение таблиц
        if 'tables' in report:
            tables_dir = detector_dir / 'tables'
            tables_dir.mkdir(exist_ok=True)
            for table_name, df in report['tables'].items():
                df.to_csv(tables_dir / f'{table_name}.csv', index=False)
        
        # Сохранение графиков
        if 'plots' in report:
            plots_dir = detector_dir / 'plots'
            plots_dir.mkdir(exist_ok=True)
            for plot_name, plot_func in report['plots'].items():
                plot_func()
                plt.savefig(plots_dir / f'{plot_name}.png')
                plt.close()
        detector.detect(data)
        return detector.generate_report()
    
    def _save_results(self, results: dict, format: str):
        ReportGenerator().save(results, format)
