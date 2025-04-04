# Разработка фреймворка единого алгоритма поиска аномалий

Эта ветка существует чисто под фрейм, все методы-скрипты будут храниться в main

 # Примерный вид структуры фрейма
anomaly_detection_framework/
│
├── core/                      # Основная логика фреймворка
│   ├── __init__.py
│   ├── base_detector.py       # Абстрактный класс детектора
│   ├── data_loader.py         # Унифицированная загрузка данных
│   ├── report_generator.py    # Генерация отчетов
│   └── utils.py               # Вспомогательные функции
│
├── detectors/                 # Конкретные реализации алгоритмов
│   ├── __init__.py
│   ├── isolation_forest.py
│   ├── statistical.py
│   ├── behavioral.py
│   └── ...
│
├── configs/                   # Конфигурации
│   └── default.yaml
│
├── outputs/                   # Результаты анализа
│   └── README.md
│
├── main.py                    # Точка входа
└── requirements.txt
