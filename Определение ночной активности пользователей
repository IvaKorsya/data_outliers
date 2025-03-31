from google.colab import drive
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from IPython.display import display
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
    
    # Запрос даты и часа для детального анализа
    while True:
        date_input = input("Введите дату для детального анализа (ГГГГ-ММ-ДД или 'нет'): ").strip()
        if date_input.lower() == 'нет':
            target_date = None
            break
        try:
            target_date = datetime.strptime(date_input, '%Y-%m-%d').date()
            break
        except ValueError:
            print("Неверный формат даты. Используйте ГГГГ-ММ-ДД или введите 'нет'.")
    
    if target_date:
        while True:
            try:
                target_hour = int(input("Введите час для анализа (0-23): "))
                if 0 <= target_hour <= 23:
                    break
                print("Час должен быть между 0 и 23.")
            except ValueError:
                print("Введите целое число от 0 до 23.")
    else:
        target_hour = None
    
    return path, target_date, target_hour

def load_all_data(folder_path):
    """Загрузка всех файлов данных с обработкой ua_is_bot"""
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
            
            # Преобразование ua_is_bot в bool
            if 'ua_is_bot' in df.columns:
                df['is_bot'] = df['ua_is_bot'].fillna(0).astype(bool)
            else:
                df['is_bot'] = False
                
            dfs.append(df)
            print(f"Успешно загружен: {file.split('/')[-1]}")
        except Exception as e:
            print(f"Ошибка при загрузке {file}: {e}")
    
    if not dfs:
        raise ValueError("Не удалось загрузить ни одного файла")
    return pd.concat(dfs, ignore_index=True)

def extended_analysis(df):
    """Расширенный анализ данных"""
    print("\n" + "="*50)
    print("Расширенный анализ данных")
    print("="*50)
    
    # 1. Общая статистика
    print(f"\nВсего записей: {len(df):,}")
    print(f"Период данных: {df['date'].min()} - {df['date'].max()}")
    print(f"Уникальных IP: {df['ip'].nunique():,}")
    print(f"Боты: {df['is_bot'].sum():,} ({df['is_bot'].mean():.1%})")
    
    # 2. Суточная активность
    daily_stats = df.groupby('date').agg(
        requests=('ip', 'size'),
        unique_ips=('ip', 'nunique'),
        bots=('is_bot', 'sum')
    )
    print("\nСуточная статистика:")
    display(daily_stats)
    
    # 3. Почасовой анализ
    hourly_stats = df.groupby('hour').agg(
        requests=('ip', 'size'),
        unique_ips=('ip', 'nunique'),
        bot_percentage=('is_bot', 'mean')
    )
    print("\nСредняя активность по часам:")
    display(hourly_stats)
    
    # 4. Визуализация
    plt.figure(figsize=(15, 10))
    
    # График 1: Суточная активность
    plt.subplot(2, 2, 1)
    daily_stats['requests'].plot(kind='bar', color='blue', alpha=0.7)
    plt.title('Общая активность по дням')
    plt.xlabel('Дата')
    plt.ylabel('Запросов')
    
    # График 2: Почасовая активность
    plt.subplot(2, 2, 2)
    hourly_stats['requests'].plot(kind='bar', color='green', alpha=0.7)
    plt.title('Средняя активность по часам')
    plt.xlabel('Час дня')
    plt.ylabel('Запросов')
    
    # График 3: Распределение ботов
    plt.subplot(2, 2, 3)
    df['is_bot'].value_counts().plot(kind='pie', autopct='%1.1f%%', 
                                   colors=['green', 'red'], 
                                   labels=['Люди', 'Боты'])
    plt.title('Распределение запросов')
    
    # График 4: Топ IP
    plt.subplot(2, 2, 4)
    top_ips = df['ip'].value_counts().head(10)
    top_ips.plot(kind='barh', color='purple', alpha=0.7)
    plt.title('Топ-10 самых активных IP')
    plt.xlabel('Количество запросов')
    
    plt.tight_layout()
    plt.show()

def analyze_night_activity(df):
    """Анализ ночной активности (00:00-07:00)"""
    print("\n" + "="*50)
    print("Анализ ночной активности (00:00 - 07:00)")
    print("="*50)
    
    night_data = df[df['hour'].between(0, 7)]
    
    if night_data.empty:
        print("\nНет данных за ночной период")
        return
    
    # Общая статистика
    print(f"\nВсего событий за ночь: {len(night_data):,}")
    print(f"Уникальных IP: {night_data['ip'].nunique():,}")
    print(f"Запросов от ботов: {night_data['is_bot'].sum():,} ({night_data['is_bot'].mean():.1%})")
    
    # Анализ по часам
    hour_stats = night_data.groupby('hour').agg(
        ips=('ip', 'nunique'),
        requests=('ip', 'size'),
        bots=('is_bot', 'sum')
    )
    print("\nАктивность по часам:")
    display(hour_stats)
    
    # Визуализация
    plt.figure(figsize=(15, 5))
    plt.subplot(1, 2, 1)
    hour_stats['requests'].plot(kind='bar', color='navy')
    plt.title('Запросы по часам (00:00-07:00)')
    plt.xlabel('Час ночи')
    
    plt.subplot(1, 2, 2)
    night_data['date'].value_counts().sort_index().plot(kind='bar', color='darkblue')
    plt.title('Распределение по дням')
    plt.tight_layout()
    plt.show()
    
    # Анализ аномалий
    analyze_anomalies(night_data, "ночной период (00:00-07:00)")

def analyze_specific_hour(df, target_date, target_hour):
    """Анализ конкретного часа в конкретную дату"""
    print("\n" + "="*50)
    print(f"Анализ активности {target_date} в {target_hour}:00")
    print("="*50)
    
    hour_data = df[(df['date'] == pd.to_datetime(target_date).date()) & 
                 (df['hour'] == target_hour)]
    
    if hour_data.empty:
        print(f"\nНет данных за {target_date} {target_hour}:00")
        return
    
    # Статистика за час
    print(f"\nВсего событий: {len(hour_data):,}")
    print(f"Уникальных IP: {hour_data['ip'].nunique():,}")
    print(f"Боты: {hour_data['is_bot'].sum():,} ({hour_data['is_bot'].mean():.1%})")
    print(f"Средняя активность: {len(hour_data)/hour_data['ip'].nunique():.1f} запросов/IP")
    
    # Топ активных IP
    ip_stats = hour_data.groupby('ip').agg(
        requests=('ip', 'size'),
        is_bot=('is_bot', 'max')
    ).sort_values('requests', ascending=False).head(10)
    
    print("\nТоп-10 активных IP:")
    display(ip_stats)
    
    # Визуализация
    plt.figure(figsize=(12, 6))
    ip_stats['requests'].plot(kind='barh', color=ip_stats['is_bot'].map({True: 'red', False: 'green'}))
    plt.title(f'Топ IP {target_date} {target_hour}:00\nКрасные - боты, Зеленые - люди')
    plt.xlabel('Количество запросов')
    plt.show()
    
    # Анализ аномалий
    analyze_anomalies(hour_data, f"{target_date} {target_hour}:00")

def analyze_anomalies(data, period_name):
    """Обнаружение аномальной активности"""
    anomalies = data.groupby(['ip', 'is_bot']).size().reset_index(name='requests')
    anomalies = anomalies[anomalies['requests'] > 100]
    
    if not anomalies.empty:
        print(f"\nОбнаружены аномалии в {period_name}:")
        print(f"IP с >100 запросами: {len(anomalies)}")
        print("Распределение:")
        display(anomalies.groupby('is_bot').agg(
            count=('ip', 'size'),
            avg_requests=('requests', 'mean'),
            max_requests=('requests', 'max')
        ))
    else:
        print(f"\nАномалий не обнаружено в {period_name}")

# Основной анализ
def main():
    print("Анализ активности пользователей и ботов")
    drive.mount('/content/drive', force_remount=True)
    
    try:
        # Получаем пользовательский ввод
        folder_path, target_date, target_hour = get_user_input()
        
        # Загрузка данных
        df = load_all_data(folder_path)
        
        # 1. Расширенный анализ
        extended_analysis(df)
        
        # 2. Анализ ночной активности
        analyze_night_activity(df)
        
        # 3. Анализ конкретного часа (если указан)
        if target_date is not None and target_hour is not None:
            analyze_specific_hour(df, str(target_date), target_hour)
        
    except Exception as e:
        print(f"\nОшибка при анализе: {e}")
    finally:
        print("\nАнализ завершен")

if __name__ == "__main__":
    main()
