# Фреймворк для обнаружения аномалий в пользовательской активности

Данный фреймворк предназначен для автоматического поиска аномалий в пользовательских логах. Он модульный, расширяемый, включает множество детекторов, поддержку отчётности, генерацию тестовых данных и гибкую CLI-интерфейсную оболочку.

# Включённые детекторы и их статус

| Детектор             | Статус    | Особенности |
|----------------------|-----------|-------------|
| `activity_spikes`    |  Работает | Анализ всплесков активности, поддерживает сопоставление с ТВ-расписанием |
| `node_id_check`      |  Работает | Ищет записи с отсутствующим `node_id`, но с признаками контента |
| `untagged_bots`      |  Работает | Детектирует скрытых ботов, обновлён под реальные User-Agent шаблоны |
| `night_activity`     |  Работает | Находит активность в ночное время и сравнивает с дневной |
| `page_view`          |  Работает | Выявляет сбои в последовательности просмотров (reset, skip) |
| `isolation_forest`   |  Работает | ML-детектор для всплесков, требует `requests`, `unique_ips`, `bot_ratio` |
| `users_devices`      |  Работает | Анализ устройств в рамках сессий, находит мультимодальные и нулевые сессии |

## 🛠️ Как установить зависимости

```bash
pip install -r requirements.txt
```
##  Советы для доработки детекторов

- Всегда наследуйся от `BaseAnomalyDetector`
- В `detect()` — вся логика и фильтрация
- В `generate_report()` — генерация таблиц, графиков, метрик
- В отчёте конвертируй всё в нативные Python-типажи: `int`, `float`, `str`
- Сначала сам сгенерируй аномалию в синкретических данных, проведи запуск и удостоверься, что находит,  потом уже ищи в датасете
# Как запустить анализ?

## 🚀 Как запустить анализ реальных данных

```bash
python main.py \
  --config core/config/local.yaml \
  --data-path data/test_activity.parquet \
  --detectors activity_spikes node_id_check \
  --output-format html \
  --output-dir outputs/reports/manual_run \
  --log-level DEBUG
```
# Как сгенерировать тестовые данные?
```
python test_data.py
```
Сгенерируется файл data/test_activity.parquet со 100k строк и следующими аномалиями:

-Всплески activity_spikes

-Пропущенные node_id

-Сбои в page_view_order_number

-Боты без ua_is_bot

-Смешанное использование устройств users_devicess

Пример тестового запуска:
```
python main.py \
  --config core/config/local.yaml \
  --data-path data/test_activity.parquet \
  --detectors activity_spikes,node_id_check,page_view \
  --output-format html \
  --log-level INFO
```
Можно использовать --detectors all, если нужно запустить всё, что включено в конфиг (enabled: true).



# Как переделать старый метод в детектор, совместимый с фреймворком

Шаги по адаптации:

 1. Создай файл core/detectors/your_method.py

 2. Создай класс, унаследованный от BaseAnomalyDetector, пример:

```
from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt

class YourMethodDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        # Прочти параметры из self.config

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        # вставь свою логику сюда
        # ВАЖНО: сохранить результат в self.results
        ...
        self.results = ...
        return data  # можно вернуть обработанный data, если нужно

    def generate_report(self) -> dict:
        if self.results is None or self.results.empty:
            return {
                "summary": "No anomalies detected.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        return {
            "summary": f"Found {len(self.results)} anomalies",
            "metrics": {
                "total": len(self.results)
            },
            "tables": {
                "anomalies": self.results
            },
            "plots": {
                "your_plot": self._plot_func
            }
        }

    def _plot_func(self):
        # вставить сюда отрисовку графика
        ...
```
 3. Перенеси ключевую логику:

Из скрипта:

- фильтрация → в detect()

- сохранение → заменить на self.results = ...

- графики → сделать функциями и вернуть в generate_report

Пример — до и после

# Было:
```
df = pd.read_parquet("data.parquet")
peaks = detect_spikes(df)
peaks.to_csv("output.csv")
```
# Стало:
```
def detect(self, data):
    ...
    self.results = peaks_df
    return data
```
4.Добавь импорт в core/detectors/__init__.py:
```
from .your_method import YourMethodDetector
__all__ = [...
           ('YourMethodDetector'),
          ....]
```
5.Добавь в main.py:
```
from core.detectors.your_method import YourMethodDetector

detectors = {
    ...
    'your_method': YourMethodDetector
}
```
6.Проверь конфигурацию в core/config/local.yaml:
```
  your_method:
    enabled: true
    threshold: 0.9
    ...
```
7. Допиши test_data.py, если нужны специальные данные(а они чаще всего нужны)

#  Структура generate_report() должна возвращать(как минимум):
```
{
  "summary": "Краткое описание результата",
  "metrics": {"метрика": значение},
  "tables": {"название": pd.DataFrame},
  "plots": {"название": функция_отрисовки}
}
```
# Как фреймворк работает внутри?

1. main.py запускает AnalysisRunner

2. Загружается дата через DataLoader

3. Для каждого детектора:

    3.1 вызывается detect()

    3.2 затем generate_report()

    3.3 сохраняется в outputs/reports/<название_детектора>/

 4. report_generator.py собирает финальный summary_report.html или .json

#  Пример финального отчёта:

- meta.json — краткое описание и параметры

- tables/*.parquet|csv — сохранённые таблицы

- plots/*.png — графики

- summary_report.html — сводный HTML отчёт

 # Cтруктуры фреймворка

```
framework_development/
│
├── main.py                    # Точка входа
├── test_data.py               # Генерация тестовых данных
├── requirements.txt
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
│   └── runner.py                   # Координатор
│
├── data/                           # Входные данные (test/реальные)
├── outputs/                        # Автосохранение результатов
│   ├── reports/                    # Готовые отчеты
└── README.md                       # Инструкции
```
