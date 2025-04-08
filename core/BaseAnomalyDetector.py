from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
import json
import matplotlib.pyplot as plt
from typing import Dict, Any, Optional

class BaseAnomalyDetector(ABC):
    def __init__(self, config: Dict[str, Any] = None):
        """
        Улучшенный базовый класс с поддержкой:
        - Стандартизированной конфигурации
        - Встроенной валидации
        - Гибкой системы отчетности
        """
        self.config = config or {}
        self.results = None
        self._validate_config()
        self._init_plots()

    @abstractmethod
    def detect(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Обновленный контракт метода:
        - Принимает DataFrame
        - Возвращает обработанные данные или None
        - Сохраняет результаты в self.results
        """
        pass

    @abstractmethod
    def generate_report(self) -> Dict[str, Any]:
        """
        Новый формат отчета:
        {
            "summary": str,
            "metrics": dict,
            "tables": Dict[str, pd.DataFrame],
            "plots": Dict[str, callable],
            "raw_data": Optional[pd.DataFrame]
        }
        """
        pass

    def save_results(self, output_dir: Path) -> None:
        """Улучшенное сохранение с обработкой ошибок"""
        if not self.results:
            raise ValueError("Detection must be run before saving results")
        
        output_dir.mkdir(parents=True, exist_ok=True)
        report = self.generate_report()

        # Сохранение метаданных
        with open(output_dir / "meta.json", "w") as f:
            json.dump({
                "detector": self.__class__.__name__,
                "config": self.config,
                "summary": report.get("summary"),
                "metrics": report.get("metrics", {})
            }, f, indent=2)

        # Сохранение таблиц
        if "tables" in report:
            tables_dir = output_dir / "tables"
            tables_dir.mkdir(exist_ok=True)
            for name, df in report["tables"].items():
                df.to_parquet(tables_dir / f"{name}.parquet")

        # Сохранение графиков
        if "plots" in report:
            plots_dir = output_dir / "plots"
            plots_dir.mkdir(exist_ok=True)
            for name, plot_func in report["plots"].items():
                try:
                    plot_func()
                    plt.savefig(plots_dir / f"{name}.png", bbox_inches='tight')
                    plt.close()
                except Exception as e:
                    print(f"Failed to save plot {name}: {str(e)}")

    def _validate_config(self) -> None:
        """Расширенная валидация конфигурации"""
        required = self.config.get("required_params", [])
        missing = [p for p in required if p not in self.config]
        if missing:
            raise ValueError(f"Missing required config parameters: {missing}")

    def _init_plots(self) -> None:
        """Инициализация стандартных настроек графиков"""
        plt.style.use('seaborn')
        self.plot_config = {
            "figure.figsize": (12, 6),
            "font.size": 12
        }
        plt.rcParams.update(self.plot_config)

    def _filter_data(self, data: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """Универсальный метод фильтрации"""
        for col, condition in filters.items():
            if col in data.columns:
                if callable(condition):
                    data = data[condition(data[col])]
                else:
                    data = data[data[col] == condition]
        return data
