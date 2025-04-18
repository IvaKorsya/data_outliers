# Разработка фреймворка единого алгоритма поиска аномалий

Эта ветка существует чисто под тестирование фрейма, все методы-скрипты будут храниться в main, фрейм в разработке.

Тестирование производится командами:
```
python main.py \
  --config core/config/local.yaml \
  --data-path data/test_activity.parquet \
  --detectors activity_spikes \
  --output-format html \
  --log-level DEBUG
```
Создание тестовых данных для проверки работы метода/методов(ещё не решил) осуществляется командой:
```
python test_data.py
```

# НЕ ПЫТАТЬСЯ СОВСМЕСТИТЬ C MAIN

Пусть пока что здесь будет дневник разработчика

Фрейм должен совмещать все методы поиска аномалий из мейна и уметь запускать их всех сразу, собирать информацию, загружать её в отчёты и складировать их в /outputs. 

Для осуществления всех этих фич сначала надо написать код для самого фрейма, то есть взаимодействие разных файлов друг с другом, которое должно запускаться скриптом main.py и осуществлять анализ всех подходящих данных.
Чтобы хоть как то объединить все придуманные методы в один фреймворк требуется их все переписать.

```
python main.py \
  --config configs/local.yaml \
  --data-path ./data \
  --detectors isolation_forest,node_id_check \
  --output-format html
```

```
python main.py \
  --config configs/production.yaml \
  --data-path /mnt/data/input \
  --detectors all \
  --output-dir /mnt/data/reports
```
всё про взаимодействие собрано в /core
 # /core
  - все детекторы и папка config перемещены сюда
  - Базовый класс BaseAnomalyDetector служит архитектурным фундаментом для системы обнаружения аномалий, то есть неким скелетом любого взятого метода, думаю придётся их всех переделать.
  - config manager нужен для загрузки значений постоянных в зависимости от нужд пользователя, от вида использования фрейма как конечного продукта
  - data_loader загружает данные в зависимости от потребностей пользователя и рассчитан на даты в имени файлов\
  - генератор отчётов делает всё точно соответствуя своему названию
  - оркестратор анализа runner.py запускает параллельный анализ, все методы
# Что делать?

a) Стандартизация детекторов

Все детекторы должны быть переписаны по шаблону:

```
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
        plt.style.use(self.config.get("plot_style", "ggplot"))
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
 ```
b)Исправление runner.py ---done

Основные исправления:

Удалить дублирование кода

Добавить проверку интерфейса детекторов

Использовать конфигурацию из ConfigManager

c) Обновление зависимостей-----# done

Дополнить requirements.txt:
```
scipy>=1.10.0
pyyaml>=6.0
```
d) Удаление Colab-зависимостей ---done
Заменить все вызовы drive.mount() на работу с локальными путями из конфига.

 # Примерный вид структуры фрейма

```│
anomaly_detection_framework/
│
│
├── core/                           # Ядро системы
    ├── config/                        # Конфигурации для всех сред
│      ├── default.yaml                # Настройки для Google Colab
│      ├── local.yaml                  # Локальные настройки
│      └── production.yaml             # Продакшен-конфиг
    ├── detectors/              
│      ├── __init__.py
│      ├── activity_spikes.py          # Анализ всплесков активности
│      ├── isolation_forest.py         # Isolation Forest
│      ├── night_activity.py           # Ночная активность
│      ├── node_id_check.py            # Проверка node_id
│      ├── page_view.py                # Аномалии просмотров
│      └── untagged_bots.py            # Неотмеченные боты
│
│   ├── base_detector.py            # Базовый класс детектора
│   ├── config_manager.py           # Загрузка конфигов
│   ├── data_loader.py              # Умный загрузчик данных
│   ├── report_generator.py         # Генерация отчетов
│   └── runner.py                   # Оркестратор анализа
│
├── outputs/                        # Автосохранение результатов
│   ├── reports/                    # Готовые отчеты
│   └── datasets/                   # Обработанные данные
│
├── main.py                         # Точка входа
├── requirements.txt                # Зависимости
└── README.md                       # Инструкции
```
