from google.colab import drive
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.ensemble import IsolationForest
from IPython.display import display, HTML
import glob
import os
from datetime import datetime

def get_user_input():
    """Функция для получения пользовательского ввода с валидацией"""
    # Запрос пути к данным
    while True:
        path = input("Введите путь к папке с данными (например: /content/drive/MyDrive/dataset): ").strip()
        if os.path.exists(path):
            break
        print(f"Ошибка: путь '{path}' не существует. Попробуйте снова.")
    
    # Запрос параметров анализа
    while True:
        try:
            interval = int(input("Введите интервал агрегации в минутах (по умолчанию 5): ") or 5)
            if interval > 0:
                break
            print("Интервал должен быть положительным числом.")
        except ValueError:
            print("Введите целое число минут.")
    
    while True:
        try:
            contamination = float(input("Введите уровень загрязнения для IsolationForest (0.01-0.5, по умолчанию 0.05): ") or 0.05)
            if 0.01 <= contamination <= 0.5:
                break
            print("Значение должно быть между 0.01 и 0.5.")
        except ValueError:
            print("Введите число с плавающей точкой.")
    
    return path, interval, contamination

def load_all_data(folder_path):
    """Загрузка и предобработка данных"""
    all_files = sorted(glob.glob(f"{folder_path}/data_2024-10-*.parquet"))
    if not all_files:  # Альтернативный вариант поиска
        all_files = sorted(glob.glob(f"{folder_path}/*.parquet"))
    
    dfs = []
    for file in all_files:
        try:
            df = pd.read_parquet(file)
            df['ts'] = pd.to_datetime(df['ts'])
            df['date'] = df['ts'].dt.date
            df['hour'] = df['ts'].dt.hour
            df['minute'] = df['ts'].dt.minute
            
            # Помечаем ботов (включая скрытых)
            if 'ua_is_bot' in df.columns:
                df['is_bot'] = np.where(pd.to_numeric(df['ua_is_bot'], errors='coerce') > 0, True, False)
            else:
                df['is_bot'] = False
                
            dfs.append(df)
            print(f"Успешно загружен: {file.split('/')[-1]}")
        except Exception as e:
            print(f"Ошибка при загрузке {file}: {e}")
    
    if not dfs:
        raise ValueError("Не удалось загрузить ни одного файла")
    return pd.concat(dfs, ignore_index=True)

def detect_anomalies(df, interval_minutes=5, contamination=0.05):
    """Поиск аномалий во временных рядах"""
    # Агрегация по заданным интервалам
    interval_str = f"{interval_minutes}min"
    df['time_interval'] = df['ts'].dt.floor(interval_str)
    activity = df.groupby('time_interval').agg(
        requests=('ip', 'count'),
        unique_ips=('ip', 'nunique'),
        bot_ratio=('is_bot', 'mean'),
        bot_count=('is_bot', 'sum'),
        human_count=('is_bot', lambda x: len(x) - sum(x))
    ).reset_index()
    
    # Метод Isolation Forest для выявления аномалий
    model = IsolationForest(contamination=contamination, random_state=42)
    anomalies = model.fit_predict(activity[['requests', 'unique_ips']])
    activity['is_anomaly'] = anomalies == -1
    
    return activity

def analyze_anomalies(activity):
    """Расширенный анализ аномалий"""
    anomaly_data = activity[activity['is_anomaly']]
    if anomaly_data.empty:
        print("\nАномалий не обнаружено")
        return
    
    print("\n" + "="*50)
    print("Детальный анализ аномалий")
    print("="*50)
    
    # 1. Общая статистика
    print(f"\nВсего аномальных интервалов: {len(anomaly_data)}")
    print(f"Среднее количество запросов в аномалиях: {anomaly_data['requests'].mean():.1f}")
    print(f"Максимальное количество запросов: {anomaly_data['requests'].max()}")
    print(f"Доля ботов в аномалиях: {anomaly_data['bot_ratio'].mean():.1%}")
    
    # 2. Топ аномалий
    print("\nТоп-5 самых значительных аномалий:")
    top_anomalies = anomaly_data.sort_values('requests', ascending=False).head(5)
    display(top_anomalies)
    
    # 3. Распределение по часам
    anomaly_data['hour'] = anomaly_data['time_interval'].dt.hour
    hour_dist = anomaly_data.groupby('hour').size()
    
    # 4. Визуализация
    plt.figure(figsize=(15, 10))
    
    # График 1: Распределение аномалий по часам
    plt.subplot(2, 2, 1)
    hour_dist.plot(kind='bar', color='orange')
    plt.title('Распределение аномалий по часам дня')
    plt.xlabel('Час')
    plt.ylabel('Количество аномалий')
    
    # График 2: Соотношение ботов/людей в аномалиях
    plt.subplot(2, 2, 2)
    plt.pie(
        [anomaly_data['bot_count'].sum(), anomaly_data['human_count'].sum()],
        labels=['Боты', 'Люди'],
        colors=['red', 'green'],
        autopct='%1.1f%%'
    )
    plt.title('Соотношение ботов и людей в аномалиях')
    
    # График 3: Запросы vs Уникальные IP
    plt.subplot(2, 2, 3)
    plt.scatter(
        activity['requests'],
        activity['unique_ips'],
        c=activity['is_anomaly'].map({True: 'red', False: 'blue'}),
        alpha=0.5
    )
    plt.xlabel('Количество запросов')
    plt.ylabel('Уникальные IP')
    plt.title('Запросы vs Уникальные IP (красные - аномалии)')
    
    # График 4: Временной ряд с аномалиями
    plt.subplot(2, 2, 4)
    plt.plot(activity['time_interval'], activity['requests'], label='Запросы', color='blue')
    plt.scatter(
        anomaly_data['time_interval'],
        anomaly_data['requests'],
        color='red', label='Аномалии'
    )
    plt.title('Временной ряд с аномалиями')
    plt.xlabel('Время')
    plt.ylabel('Запросы')
    plt.legend()
    
    plt.tight_layout()
    plt.show()

def save_results(activity, anomaly_data, folder_path):
    """Сохранение результатов анализа"""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Сохранение данных
    activity.to_csv(f"{folder_path}/activity_data_{timestamp}.csv", index=False)
    if not anomaly_data.empty:
        anomaly_data.to_csv(f"{folder_path}/anomalies_{timestamp}.csv", index=False)
    
    # Сохранение графиков
    plt.savefig(f"{folder_path}/anomaly_analysis_{timestamp}.png")
    print(f"\nРезультаты сохранены в папку: {folder_path}")

# Основной анализ
def main():
    print("Анализ аномалий в данных активности")
    drive.mount('/content/drive', force_remount=True)
    
    try:
        # Получаем пользовательский ввод
        folder_path, interval, contamination = get_user_input()
        
        # Загрузка данных
        df = load_all_data(folder_path)
        
        # Поиск аномалий
        activity = detect_anomalies(df, interval, contamination)
        
        # Анализ и визуализация
        analyze_anomalies(activity)
        
        # Сохранение результатов
        output_folder = os.path.join(os.path.dirname(folder_path), "anomaly_results")
        save_results(activity, activity[activity['is_anomaly']], output_folder)
        
    except Exception as e:
        print(f"\nОшибка при анализе: {e}")
    finally:
        print("\nАнализ завершен")

if __name__ == "__main__":
    main()
