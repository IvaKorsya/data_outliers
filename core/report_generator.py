import matplotlib.pyplot as plt
from IPython.display import display
import pandas as pd

class ReportGenerator:
    @staticmethod
    def generate(detector_name: str, report_data: dict, save_path: str = None):
        """Генерация стандартного отчета"""
        print(f"\n{'='*50}")
        print(f"Report for {detector_name}")
        print("="*50)
        
        # Текстовая часть
        print("\nSUMMARY:")
        print(report_data.get('summary', ''))
        
        # Таблицы
        if 'tables' in report_data:
            for table_name, table_data in report_data['tables'].items():
                print(f"\n{table_name}:")
                display(table_data)
        
        # Графики
        if 'plots' in report_data:
            for plot_name, plot_func in report_data['plots'].items():
                print(f"\nGenerating plot: {plot_name}")
                plot_func()
                plt.show()
        
        # Сохранение
        if save_path:
            ReportGenerator._save_report(report_data, save_path)
