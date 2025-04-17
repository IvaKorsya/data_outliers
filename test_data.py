# create_test_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Генерация тестовых данных
start_date = datetime.now() - timedelta(days=1)
timestamps = [start_date + timedelta(minutes=i) for i in range(1440)]
events = ['page_view']*1200 + ['click']*240  # 1200 просмотров и 240 кликов

df = pd.DataFrame({
    'ts': timestamps,
    'event': np.random.choice(events, size=1440),
    'user_id': np.random.randint(1000, 1100, size=1440),
    'requests': np.random.poisson(50, size=1440)  # Базовый уровень активности
})

# Добавляем искусственные аномалии
anomaly_ranges = [
    (100, 120, 500),   # Пик с 100 по 120 минуту (500 запросов)
    (300, 301, 1000),  # Резкий скачок на 300 минуте
    (600, 605, 800),   # Плато повышенной активности
    (1000, 1010, 10)   # Аномально низкая активность
]

for start, end, value in anomaly_ranges:
    df.loc[start:end, 'requests'] = value

# Добавляем случайные выбросы
outlier_indices = np.random.choice(1440, size=20, replace=False)
df.loc[outlier_indices, 'requests'] = np.random.randint(300, 1000, size=20)

# Сохранение
df.to_parquet('data/test_activity.parquet')

# Тестовое расписание (для анализа соответствия)
schedule = pd.DataFrame({
    'start_ts': [start_date + timedelta(hours=i) for i in range(24)],
    'dur': [3600]*24,  # 1 час
    'title': [f'Program {i}' for i in range(24)],
    'event_type': ['show']*24,
    'channel_id': [i%5 + 1 for i in range(24)],
    'expected_activity': np.random.poisson(200, size=24)  # Ожидаемая активность
})
schedule.to_csv('data/tv_schedule.csv', index=False)
print("Сгенерированы аномалии:")
for start, end, value in anomaly_ranges:
    print(f"- Период: {start}-{end} мин, значение: {value}")
print(f"Случайные выбросы: {len(outlier_indices)} точек")