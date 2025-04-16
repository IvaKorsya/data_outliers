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
    'user_id': np.random.randint(1000, 1100, size=1440)
})

# Сохранение
df.to_parquet('data/test_activity.parquet')

# Тестовое расписание (если нужно)
schedule = pd.DataFrame({
    'start_ts': [start_date + timedelta(hours=i) for i in range(24)],
    'dur': [3600]*24,  # 1 час
    'title': [f'Program {i}' for i in range(24)],
    'event_type': ['show']*24,
    'channel_id': [i%5 + 1 for i in range(24)]
})
schedule.to_csv('data/tv_schedule.csv', index=False)
