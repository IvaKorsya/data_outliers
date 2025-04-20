import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string

def generate_random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# === БАЗОВАЯ АКТИВНОСТЬ ДЛЯ activity_spikes ===
start_date = datetime.now() - timedelta(days=1)
timestamps = [start_date + timedelta(minutes=i) for i in range(1440)]
events = ['page_view'] * 1200 + ['click'] * 240

df = pd.DataFrame({
    'ts': timestamps,
    'event': np.random.choice(events, size=1440),
    'user_id': np.random.randint(1000, 1100, size=1440),
    'requests': np.random.poisson(50, size=1440),
    'unique_ips': np.random.poisson(30, size=1440),
    'bot_ratio': np.random.uniform(0, 0.2, size=1440),
    'ip': [f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}" for _ in range(1440)]
})

# === АНОМАЛИИ ДЛЯ activity_spikes ===
anomaly_ranges = [
    (100, 120, 500), (300, 301, 1000), (600, 605, 800), (1000, 1010, 10)
]
for start, end, value in anomaly_ranges:
    df.loc[start:end, 'requests'] = value
    df.loc[start:end, 'unique_ips'] = value * 0.6
    df.loc[start:end, 'bot_ratio'] = np.random.uniform(0.3, 0.9, size=end - start + 1)

# === node_id_check ===
urls = [f"https://example.com/{generate_random_string(4)}" for _ in range(20)]
content_types = ['article', 'video', 'image', 'product']
node_data = []
for url in urls:
    if random.random() < 0.1:  # 10% конфликтов
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
node_df = pd.DataFrame(node_data * 10)  # расширим объём

# === ДОБАВКА: ДАННЫЕ ДЛЯ untagged_bots ===
bot_ips = [f"10.0.0.{i}" for i in range(5)]  # 5 ботов
bot_entries = []
for ip in bot_ips:
    for _ in range(150):
        bot_entries.append({
            'ts': start_date + timedelta(seconds=np.random.randint(0, 86400)),
            'event': 'page_view',
            'user_id': np.random.randint(2000, 2100),
            'requests': np.random.poisson(20),
            'unique_ips': np.random.poisson(10),
            'bot_ratio': np.random.uniform(0.4, 0.9),
            'ip': ip,
            'ua_is_bot': 0,
            'ua_header': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
        })

clean_ips = [f"192.0.2.{i}" for i in range(10)]
for ip in clean_ips:
    for _ in range(20):
        bot_entries.append({
            'ts': start_date + timedelta(seconds=np.random.randint(0, 86400)),
            'event': 'click',
            'user_id': np.random.randint(3000, 3100),
            'requests': np.random.poisson(2),
            'unique_ips': np.random.poisson(1),
            'bot_ratio': np.random.uniform(0, 0.05),
            'ip': ip,
            'ua_is_bot': 0,
            'ua_header': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        })

bot_df = pd.DataFrame(bot_entries)
df = pd.concat([df, bot_df], ignore_index=True)

# === ФИНАЛЬНОЕ ОБЪЕДИНЕНИЕ ДЛЯ ВСЕХ ===
df['ts'] = pd.to_datetime(df['ts']) + pd.to_timedelta(np.arange(len(df)), unit='s')
final_df = pd.merge(df, node_df, left_index=True, right_index=True, how='left', suffixes=('', '_node'))
final_df['ts'] = final_df['ts']
final_df.drop(['ts_node'], axis=1, inplace=True, errors='ignore')

# === СОХРАНЕНИЕ ===
final_df.to_parquet('data/test_activity.parquet')

# === ТВ-РАСПИСАНИЕ ДЛЯ activity_spikes ===
schedule = pd.DataFrame({
    'start_ts': [start_date + timedelta(hours=i) for i in range(24)],
    'dur': [3600] * 24,
    'title': [f'Program {i}' for i in range(24)],
    'event_type': ['show'] * 24,
    'channel_id': [i % 5 + 1 for i in range(24)],
    'expected_activity': np.random.poisson(200, size=24)
})
schedule.to_csv('data/tv_schedule.csv', index=False)

# === ЛОГ ===
print("✅ Generated test data with:")
print(f"• Activity spikes: {anomaly_ranges}")
print(f"• node_id conflicts: 10% URLs with multiple node_ids")
print(f"• Hidden bots: {len(bot_ips)} IPs × 150 req")
print(f"• Clean users: {len(clean_ips)} IPs × 20 req")
print("📁 Saved to: data/test_activity.parquet")
