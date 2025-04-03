# Документация проекта "Stranger Things"

Общее описание проекта:

Проект "Stranger Things" разрабатывает универсальный алгоритм для поиска аномалий в данных, включая работу в режиме, близком к реальному времени. Проект выполняется в рамках курса ОПД 2025 СПБПУ и использует данные по активности пользователей на сайте МАТЧ ТВ.

Основные модули:
1. activity_spikes_analysis.py - анализ всплесков активности
2. anomaly_without_tag_bot.py - обнаружение ботов
3. activity_spikes_isolation.py - поиск аномалий методом Isolation Forest
4. night_activity_analysis.py - анализ ночной активности
5. page_view_anomalies.py - проверка аномалий, связанных с просмотром страниц
6. node_id_check - проверка id видео
7. device_of_user.ipynb  - проверка устройств, с которых выполняются действия     
   
    # activity_spikes_analysis.py
   
```
    def analyze_data(dataset_path: str, schedule_file: str) -> None:
Параметры:
        dataset_path (str): Путь к .parquet файлам данных
        schedule_file (str): Путь к CSV с телепрограммой
    
    Возвращает:
        None: Выводит графики и результаты в консоль
    
    Пример:
        >>> analyze_data("data/*.parquet", "tv_schedule.csv")
 
```
# anomaly_without_tag_bot.py
```
class BotDetector:
   
    Класс для обнаружения скрытых ботов по поведенческим паттернам.
    
    Атрибуты:
        data (pd.DataFrame): Загруженные данные активности
        bot_threshold (int): Порог запросов для определения бота (по умолч. 100)
   
    def __init__(self, data_folder: str):
        """Инициализация с загрузкой данных из указанной папки"""
        self.data = self._load_data(data_folder)
        self.bot_threshold = 100
    
    def detect_hidden_bots(self) -> pd.DataFrame:
        """Возвращает DataFrame с помеченными ботами"""
```
# activity_spikes_isolation.py
```
def detect_anomalies(
    data_path: str,
    interval: int = 5,
    contamination: float = 0.05
) -> dict:
    
    Обнаружение аномалий методом Isolation Forest.
    
    Параметры:
        data_path (str): Путь к данным
        interval (int): Интервал агрегации в минутах (5)
        contamination (float): Уровень загрязнения (0.05)
    
    Возвращает:
        dict: {
            'stats': общая статистика,
            'plots': пути к графикам,
            'top_anomalies': топ аномалий
        }
```
# night_activity_analysis.py
```
def analyze_night_activity(
    data: pd.DataFrame,
    start_hour: int = 0,
    end_hour: int = 7
) -> pd.DataFrame:
    """
    Анализирует активность в ночной период.
    
    Параметры:
        data (pd.DataFrame): Данные активности
        start_hour (int): Начальный час (0)
        end_hour (int): Конечный час (7)
    
    Возвращает:
        pd.DataFrame: Статистика по ночной активности
    
    Исключения:
        ValueError: Если данные не содержат ночных записей
    """
    # Реализация функции..
    
```
# page_view_anomalies.py
```
def load_and_preprocess_data(file_path) 
-> pd.DataFrame

Параметры:
    file_path (str): Путь к файлу с данными

Возвращает:
    pd.DataFrame: Обработанный DataFrame
    
    
def detect_page_number_anomalies(
df: pd.DataFrame,
user_id_column='randPAS_user_agent_id': str,
session_id_column='randPAS_session_id': str
) -> pd.DataFrame

Находит аномалии в нумерации page_view_order_number:
    - reset: текущий номер < предыдущего (например, 3 → 1),
    - skip: текущий номер - предыдущий > 1 (например, 2 → 4),

Параметры:
    df (pd.DataFrame): DataFrame с данными
    user_id_column (str): Название колонки с ID пользователя
    session_id_column (str): Название колонки с ID сессии

Возвращает:
    pd.DataFrame: DataFrame с аномалиям
    

def visualize_anomalies(
anomalies_df: pd.DataFrame, 
total_records: int) 

Создает визуализации для анализа аномалий:
    1. Круговую диаграмму распределения типов аномалий
    2. Соотношение нормальных и аномальных записей

Параметры:
    anomalies_df (pd.DataFrame): DataFrame с аномалиями
    total_records (int): Общее количество записей для расчета соотношения
    
```
# Установка и использование
```
# Клонирование репозитория
git clone https://github.com/IvaKorsya/data_outliers.git
# Установка зависимостей
pip install -r requirements.txt
# Запуск анализа
python Анализ всплесков и спадов и сопоставление с телепрограммой.py \
    --dataset "data/*.parquet" \
    --schedule "tv_schedule.csv"
```
# Структура проекта
```
data_outliers/
├── code/
│   ├── activity_spikes_analysis.py                                              # Анализ всплесков
│   ├── anomaly_without_tag_bot.py                                               # Детектор ботов
│   ├── activity_spikes_isolation.py                                             # Поиск аномалий активности
│   ├── night_activity_analysis.py                                               # Ночной анализ
│   ├── page_view_anomalies.py                                                   # Анализ порядка просмотра страниц
│   ├── node_id_check                                                            # Анализ id видео и тегов
│   ├── device_of_user.ipynb                                                     # Анализ  количества устройств пользователей
├── README.md                                                                    # Документация
├── .gitignore
├── requirements.txt                        
