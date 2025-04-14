# Разработка фреймворка единого алгоритма поиска аномалий

Эта ветка существует чисто под фрейм, все методы-скрипты будут храниться в main

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
  - Базовый класс BaseAnomalyDetector служит архитектурным фундаментом для системы обнаружения аномалий, то есть неким скелетом любого взятого метода, думаю придётся их всех переделать.
  - config manager нужен для загрузки значений постоянных в зависимости от нужд пользователя, от вида использования фрейма как конечного продукта
  - data_loader загружает данные в зависимости от потребностей пользователя и рассчитан на даты в имени файлов\
  - генератор отчётов делает всё точно соответствуя своему названию
  - оркестратор анализа runner.py запускает параллельный анализ, все методы
# Что делать?

a) Стандартизация детекторов

Все детекторы должны быть переписаны по шаблону:

```
from core.base_detector import BaseAnomalyDetector
import pandas as pd

class MyDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        # Инициализация параметров из config
        
    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        # Логика обнаружения аномалий
        self.results = processed_data
        return self.results
        
    def generate_report(self) -> dict:
        return {
            "summary": "Report summary",
            "metrics": {...},
            "tables": {...},
            "plots": {...}
        }
 ```
b)Исправление runner.py

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
d) Удаление Colab-зависимостей
Заменить все вызовы drive.mount() на работу с локальными путями из конфига.

 # Примерный вид структуры фрейма

anomaly_detection_framework/

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
