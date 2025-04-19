import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string

def generate_random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Генерация основной активности
start_date = datetime.now() - timedelta(days=1)
timestamps = [start_date + timedelta(minutes=i) for i in range(1440)]
events = ['page_view']*1200 + ['click']*240

df = pd.DataFrame({
    'ts': timestamps,
    'event': np.random.choice(events, size=1440),
    'user_id': np.random.randint(1000, 1100, size=1440),
    'requests': np.random.poisson(50, size=1440),
    'unique_ips': np.random.poisson(30, size=1440),
    'bot_ratio': np.random.uniform(0, 0.2, size=1440),
    'ip': [f"192.168.{random.randint(0,255)}.{random.randint(0,255)}" for _ in range(1440)]
})

# Добавляем искусственные аномалии активности
anomaly_ranges = [
    (100, 120, 500), (300, 301, 1000), (600, 605, 800), (1000, 1010, 10)
]
for start, end, value in anomaly_ranges:
    df.loc[start:end, 'requests'] = value
    df.loc[start:end, 'unique_ips'] = value * 0.6
    df.loc[start:end, 'bot_ratio'] = np.random.uniform(0.3, 0.9, size=end-start+1)

# Генерация данных для проверки node_id
urls = [f"https://example.com/{generate_random_string(4)}" for _ in range(20)]
content_types = ['article', 'video', 'image', 'product']

# Создаем проблемные данные (10% URL с несколькими node_id)
node_data = []
for url in urls:
    if random.random() < 0.1:  # 10% проблемных URL
        node_ids = [generate_random_string(8) for _ in range(2)]
    else:
        node_ids = [generate_random_string(8)]
    
    for node_id in node_ids:
        node_data.append({
            'url': url,
            'node_id': node_id,
            'content_type': random.choice(content_types),
            'title': f"Content {generate_random_string(6)}"
        })

# Собираем финальный DataFrame
node_df = pd.DataFrame(node_data * 10)  # Умножаем для объема
df['ts'] = pd.to_datetime(start_date) + pd.to_timedelta(np.arange(len(df)), unit='min')

# Объединяем с основными данными активности
final_df = pd.merge(
    df,
    node_df,
    left_index=True,
    right_index=True,
    how='left'
)

# Сохранение данных
final_df = pd.merge(
    df,
    node_df,
    left_index=True,
    right_index=True,
    how='left',
    suffixes=('', '_node')  # Убираем суффиксы для ts
)

# Оставляем только один столбец ts
final_df['ts'] = final_df['ts']  # или final_df['ts_node'], если нужны другие метки
final_df.drop(['ts_node'], axis=1, inplace=True, errors='ignore')

final_df.to_parquet('data/test_activity.parquet')
# ===== Page View Order Test Data =====
session_ids = [f"session_{i}" for i in range(100)]
user_ids = [f"user_{i}" for i in range(30)]

pageview_data = []
for session in session_ids:
    uid = np.random.choice(user_ids)
    length = np.random.randint(5, 15)
    numbers = list(range(1, length + 1))

    # Внедрение аномалий
    if np.random.rand() < 0.2:
        anomaly_type = np.random.choice(["reset", "skip"])
        if anomaly_type == "reset" and len(numbers) > 3:
            numbers[3] = 1
        elif anomaly_type == "skip" and len(numbers) > 3:
            numbers[2] += 2

    for i, val in enumerate(numbers):
        pageview_data.append({
            "randPAS_user_agent_id": uid,
            "randPAS_session_id": session,
            "page_view_order_number": val,
            "ua_is_bot": 0
        })

pageview_df = pd.DataFrame(pageview_data)
final_df = pd.concat([final_df, pageview_df], ignore_index=True)
final_df.to_parquet('data/test_activity.parquet')
# Тестовое расписание
schedule = pd.DataFrame({
    'start_ts': [start_date + timedelta(hours=i) for i in range(24)],
    'dur': [3600]*24,
    'title': [f'Program {i}' for i in range(24)],
    'event_type': ['show']*24,
    'channel_id': [i%5 + 1 for i in range(24)],
    'expected_activity': np.random.poisson(200, size=24)
})
schedule.to_csv('data/tv_schedule.csv', index=False)

print("Generated test data with:")
print("- Activity spikes at:", anomaly_ranges)
print("- 10% URLs with multiple node_ids")
print("- TV schedule for activity matching")