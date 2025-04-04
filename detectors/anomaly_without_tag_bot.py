from google.colab import drive
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from IPython.display import display
import glob
import os

# Монтирование Google Drive
drive.mount('/content/drive', force_remount=True)

def get_user_path():
    """Запрашивает путь у пользователя с проверкой существования"""
    while True:
        path = input("Введите путь к папке с данными (например: /content/drive/MyDrive/dataset): ").strip()
        if os.path.exists(path):
            return path
        print(f"Ошибка: путь '{path}' не существует. Попробуйте снова.")

def load_all_data(folder_path):
    """Загрузка всех файлов данных с безопасной обработкой"""
    all_files = sorted(glob.glob(f"{folder_path}/data_2024-10-*.parquet"))
    if not all_files:
        all_files = sorted(glob.glob(f"{folder_path}/*.parquet"))  # Альтернативный вариант поиска
    
    dfs = []
    
    for file in all_files:
        try:
            df = pd.read_parquet(file)
            # Обязательные преобразования
            df['ts'] = pd.to_datetime(df['ts'])
            df['date'] = df['ts'].dt.date
            df['hour'] = df['ts'].dt.hour
            
            # Определение ботов (включая скрытых)
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

def detect_hidden_bots(df):
    """Выявление скрытых ботов по поведенческим признакам"""
    # Признаки ботов:
    # 1. Слишком много запросов с одного IP
    ip_request_counts = df['ip'].value_counts().rename('request_count')
    df = df.merge(ip_request_counts.to_frame(), left_on='ip', right_index=True)
    
    # 2. Отсутствие User-Agent или подозрительные UA
    if 'ua_header' in df.columns:
        df['suspicious_ua'] = df['ua_header'].str.contains('bot|spider|crawl', case=False, na=False)
    
    # 3. Слишком высокая частота запросов
    df['is_hidden_bot'] = (df['is_bot'] == False) & (
        (df['request_count'] > 100) |
        (df.get('suspicious_ua', False))
    )
    
    df['is_bot'] = df['is_bot'] | df['is_hidden_bot']
    return df

def print_top_bots(df, top_n=10):
    """Вывод топ-N самых активных ботов"""
    bot_activity = df[df['is_bot']].groupby('ip').agg(
        total_requests=('request_count', 'max'),
        first_seen=('ts', 'min'),
        last_seen=('ts', 'max'),
        is_hidden=('is_hidden_bot', 'any')
    ).sort_values('total_requests', ascending=False).head(top_n)
    
    print(f"\n{'='*50}\nТоп-{top_n} самых активных ботов\n{'='*50}")
    for i, (ip, row) in enumerate(bot_activity.iterrows(), 1):
        print(f"{i}. IP: {ip}")
        print(f"   Запросов: {row['total_requests']:,}")
        print(f"   Период активности: {row['first_seen']} — {row['last_seen']}")
        print(f"   Тип: {'скрытый' if row['is_hidden'] else 'явный'}")
        print("-"*60)

def analyze_activity(df):
    """Расширенный анализ активности с визуализацией"""
    print(f"\n{'='*50}\nОбщая статистика\n{'='*50}")
    print(f"Всего записей: {len(df):,}")
    print(f"Период данных: {df['date'].min()} — {df['date'].max()}")
    print(f"Уникальных IP: {df['ip'].nunique():,}")
    
    # Статистика по ботам
    total_bots = df['is_bot'].sum()
    hidden_bots = df['is_hidden_bot'].sum()
    
    print(f"\nОбнаружено ботов: {total_bots:,} ({total_bots/len(df):.1%})")
    print(f"Из них скрытых: {hidden_bots:,} ({hidden_bots/len(df):.1%})")
    
    # Визуализация
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))
    
    # График распределения
    ax1.bar(['Люди', 'Боты (явные)', 'Боты (скрытые)'],
            [len(df) - total_bots, total_bots - hidden_bots, hidden_bots],
            color=['green', 'red', 'orange'])
    ax1.set_title('Распределение запросов')
    ax1.set_ylabel('Количество запросов')
    
    # График активности по часам
    hourly_activity = df.groupby('hour').size()
    hourly_activity.plot(kind='bar', ax=ax2, color='blue', alpha=0.7)
    ax2.set_title('Активность по часам')
    ax2.set_xlabel('Час дня')
    ax2.set_ylabel('Запросов')
    
    plt.tight_layout()
    plt.show()
    
    # Вывод топ ботов
    print_top_bots(df)

# Основной процесс анализа
try:
    print("Анализ активности и обнаружение ботов")
    folder_path = get_user_path()
    df = load_all_data(folder_path)
    df = detect_hidden_bots(df)
    analyze_activity(df)

except Exception as e:
    print(f"\nОшибка при анализе: {e}")
finally:
    print("\nАнализ завершен")
