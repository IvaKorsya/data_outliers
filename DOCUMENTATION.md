# Документация проекта "Stranger Things"

Общее описание проекта:

Проект "Stranger Things" разрабатывает универсальный алгоритм для поиска аномалий в данных, включая работу в режиме, близком к реальному времени. Проект выполняется в рамках курса ОПД 2025 СПБПУ и использует данные по активности пользователей на сайте МАТЧ ТВ.

Основные модули:
1. analyze_peaks_with_schedule.py - анализ всплесков активности
2. detect_hidden_bots.py - обнаружение ботов
3. isolation_forest_anomalies.py - поиск аномалий методом Isolation Forest
4. night_activity_analysis.py - анализ ночной активности

def analyze_data(dataset_path: str, schedule_file: str) -> None:
    """
    Анализирует всплески активности и сопоставляет с телепрограммой.
    
    Параметры:
        dataset_path (str): Путь к .parquet файлам данных
        schedule_file (str): Путь к CSV с телепрограммой
    
    Возвращает:
        None: Выводит графики и результаты в консоль
    
    Пример:
        >>> analyze_data("data/*.parquet", "tv_schedule.csv")
    """
    # Реализация функции...


```analyze_data("/path/to/dataset/*.parquet", "/path/to/tv_schedule.csv")```


