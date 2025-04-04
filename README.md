# Разработка фреймворка единого алгоритма поиска аномалий

Эта ветка существует чисто под фрейм, все методы-скрипты будут храниться в main

 # Примерный вид структуры фрейма
anomaly_detection_framework/

```│
anomaly_detection_framework/
│
├── configs/                   # Конфигурации для всех сред
│   ├── colab.yaml            # Настройки для Google Colab
│   ├── local.yaml            # Локальные настройки
│   └── production.yaml       # Продакшен-конфиг
│
├── core/                     # Ядро системы
│   ├── base_detector.py      # Базовый класс детектора
│   ├── config_manager.py     # Загрузка конфигов
│   ├── data_loader.py        # Умный загрузчик данных
│   ├── report_generator.py   # Генерация отчетов
│   └── runner.py             # Оркестратор анализа
│
├── detectors/              
│   ├── __init__.py
│   ├── activity_spikes.py    # Анализ всплесков активности
│   ├── isolation_forest.py   # Isolation Forest
│   ├── night_activity.py     # Ночная активность
│   ├── node_id_check.py      # Проверка node_id
│   ├── page_view.py          # Аномалии просмотров
│   └── untagged_bots.py      # Неотмеченные боты
│
├── outputs/                  # Автосохранение результатов
│   ├── reports/              # Готовые отчеты
│   └── datasets/             # Обработанные данные
│
├── main.py                   # Точка входа
├── requirements.txt          # Зависимости
└── README.md                 # Инструкции
```
