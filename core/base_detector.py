from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
import json
import matplotlib.pyplot as plt
from typing import Dict, Any, Optional, Union
import logging

class BaseAnomalyDetector(ABC):
    """Абстрактный базовый класс для всех детекторов аномалий.
    
    Требования к наследникам:
    1. Реализация методов detect() и generate_report()
    2. Сохранение результатов в self.results
    3. Использование self.config для параметров
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """Инициализация с валидацией конфигурации.
        
        Args:
            config: Словарь с параметрами детектора. Может содержать:
                   - required_params: список обязательных параметров
                   - plot_settings: настройки графиков
                   - любые специфичные для детектора параметры
        """
        self.config = config or {}
        self.results = None  # Для хранения результатов detect()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self._validate_config()
        self._init_plot_settings()
        
    @abstractmethod
    def detect(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Основной метод обнаружения аномалий.
        
        Args:
            data: Входные данные для анализа
            
        Returns:
            Обработанные данные (не обязательно) или None
            Обязательно сохраняет результаты в self.results
        """
        pass

    @abstractmethod
    def generate_report(self) -> Dict[str, Any]:
        """Генерация стандартизированного отчета.
        
        Returns:
            Словарь с обязательными ключами:
            - summary: str
            - metrics: dict
            - tables: Dict[str, pd.DataFrame]
            - plots: Dict[str, callable]
            - raw_data: Optional[pd.DataFrame]
        """
        pass

    def save_results(self, output_dir: Union[str, Path]) -> None:
        """Сохранение результатов в стандартизированную структуру папок.
        
        Args:
            output_dir: Путь для сохранения (создастся автоматически)
        """
        output_dir = Path(output_dir)
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            
            report = self.generate_report()
            if not report:
                raise ValueError("Empty report generated")
                
            self._save_metadata(output_dir, report)
            self._save_tables(output_dir, report)
            self._save_plots(output_dir, report)
            
        except Exception as e:
            self.logger.error(f"Failed to save results: {str(e)}")
            raise

    def _validate_config(self) -> None:
        """Проверка обязательных параметров конфигурации."""
        required_params = self.config.get("required_params", [])
        missing = [p for p in required_params if p not in self.config]
        if missing:
            error_msg = f"Missing required config parameters: {missing}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def _init_plot_settings(self) -> None:
        """Инициализация настроек графиков."""
        plt.style.use(self.config.get("plot_style", "seaborn"))
        self.plot_config = {
            "figure.figsize": self.config.get("figure_size", (12, 6)),
            "font.size": self.config.get("font_size", 12),
            "axes.titlesize": self.config.get("title_size", 14)
        }
        plt.rcParams.update(self.plot_config)

    def _save_metadata(self, output_dir: Path, report: Dict) -> None:
        """Сохранение метаданных отчета."""
        meta = {
            "detector": self.__class__.__name__,
            "timestamp": pd.Timestamp.now().isoformat(),
            "config": self.config,
            "summary": report.get("summary"),
            "metrics": report.get("metrics", {})
        }
        with open(output_dir / "meta.json", "w") as f:
            json.dump(meta, f, indent=2)

    def _save_tables(self, output_dir: Path, report: Dict) -> None:
        """Сохранение табличных данных."""
        if "tables" in report and report["tables"]:
            tables_dir = output_dir / "tables"
            tables_dir.mkdir(exist_ok=True)
            
            for name, df in report["tables"].items():
                if not isinstance(df, pd.DataFrame):
                    continue
                path = tables_dir / f"{name}.parquet"
                df.to_parquet(path)
                self.logger.debug(f"Saved table: {path}")

    def _save_plots(self, output_dir: Path, report: Dict) -> None:
        """Сохранение графиков."""
        if "plots" in report and report["plots"]:
            plots_dir = output_dir / "plots"
            plots_dir.mkdir(exist_ok=True)
            
            for name, plot_func in report["plots"].items():
                try:
                    plt.figure()
                    plot_func()
                    plot_path = plots_dir / f"{name}.png"
                    plt.savefig(plot_path, bbox_inches="tight")
                    plt.close()
                    self.logger.debug(f"Saved plot: {plot_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to save plot {name}: {str(e)}")

    def _filter_data(
        self,
        data: pd.DataFrame,
        filters: Dict[str, Any],
        drop_invalid: bool = True
    ) -> pd.DataFrame:
        """Метод фильтрации данных.
        
        Args:
            data: Исходный DataFrame
            filters: Словарь {колонка: условие}
                    Условие может быть значением или функцией
            drop_invalid: Удалять строки с NaN в фильтруемых колонках
        """
        filtered = data.copy()
        for col, condition in filters.items():
            if col not in filtered.columns:
                if drop_invalid:
                    filtered = filtered.drop(columns=[col])
                continue
                
            if callable(condition):
                filtered = filtered[condition(filtered[col])]
            else:
                filtered = filtered[filtered[col] == condition]
                
        return filtered
