from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
import json

class BaseAnomalyDetector(ABC):
    def __init__(self, config: dict = None):
        """
        Базовый класс для всех детекторов аномалий.
        
        :param config: Словарь с настройками детектора
        """
        self.config = config or {}
        self.results = None
        self._validate_config()

    @abstractmethod
    def detect(self, data: pd.DataFrame) -> dict:
        """
        Основной метод обнаружения аномалий.
        Должен возвращать словарь с сырыми результатами.
        """
        pass

    @abstractmethod
    def generate_report(self) -> dict:
        """
        Генерация стандартизированного отчета.
        Формат:
        {
            "summary": str,
            "tables": {name: DataFrame},
            "plots": {name: callable},
            "metrics": dict
        }
        """
        pass

    def save_results(self, output_dir: str):
        """
        Стандартное сохранение результатов в указанную папку.
        Автоматически создает:
        - report.json
        - report.html
        - plots/
        - data/ (при необходимости)
        """
        if not self.results:
            raise ValueError("Сначала выполните detect()")
            
        report = self.generate_report()
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Сохранение JSON
        with open(output_path / "report.json", "w") as f:
            json.dump({
                "summary": report.get("summary"),
                "metrics": report.get("metrics", {})
            }, f, indent=2)
        
        # Сохранение таблиц
        if "tables" in report:
            tables_dir = output_path / "tables"
            tables_dir.mkdir(exist_ok=True)
            for name, df in report["tables"].items():
                df.to_csv(tables_dir / f"{name}.csv", index=False)
        
        # Сохранение графиков
        if "plots" in report:
            plots_dir = output_path / "plots"
            plots_dir.mkdir(exist_ok=True)
            import matplotlib.pyplot as plt
            
            for name, plot_func in report["plots"].items():
                plot_func()
                plt.savefig(plots_dir / f"{name}.png")
                plt.close()
        
        print(f"Результаты сохранены в {output_dir}")

    def _validate_config(self):
        """Валидация конфигурации детектора"""
        required_params = self.config.get("required_params", [])
        missing = [p for p in required_params if p not in self.config]
        if missing:
            raise ValueError(f"Отсутствуют обязательные параметры: {missing}")
