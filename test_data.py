import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string
import os

# === Параметры генерации ===
NUM_ROWS = 100_000
START_DATE = datetime.now() - timedelta(days=1)

def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# === Распределение событий с акцентом на page_view ===
events_distribution = (
    ['page_view'] * 40_000 +
    ['page_ping'] * 50_000 +
    random.choices(
        ['page_scroll', 'external_link_click', 'impression_view', 'login_signin',
         'login_signout', 'pay_start', 'pay_confirm', 'subscription_cancel_view'],
        k=10_000
    )
)
random.shuffle(events_distribution)

# === Основной датафрейм ===
ts_list = [START_DATE + timedelta(seconds=i * 3) for i in range(NUM_ROWS)]
ips = [f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}" for _ in range(NUM_ROWS)]

df = pd.DataFrame({
    'ts': ts_list,
    'ip': ips,
    'event': events_distribution,
    'page_view_order_number': np.random.randint(1, 10, size=NUM_ROWS),
    'event_order_number': np.random.randint(1, 100, size=NUM_ROWS),
    'secs': np.random.randint(0, 60, size=NUM_ROWS),
    'url': [f"https://example.com/{random_string(5)}" for _ in range(NUM_ROWS)],
    'referer': [f"https://google.com/{random_string(5)}" for _ in range(NUM_ROWS)],
    'randPAS_user_passport_id': [random_string(10) for _ in range(NUM_ROWS)],
    'randPAS_user_agent_id': [random_string(15) for _ in range(NUM_ROWS)],
    'randPAS_trex_cid': [random_string(8) for _ in range(NUM_ROWS)],
    'randPAS_uma_media_cid': [random_string(8) for _ in range(NUM_ROWS)],
    'randPAS_session_id': [random_string(12) for _ in range(NUM_ROWS)],
    'is_new_page': np.random.choice([0, 1], size=NUM_ROWS, p=[0.8, 0.2]),
    'title': [f"Title {random.randint(1, 1000)}" for _ in range(NUM_ROWS)],
    'node_id': np.random.randint(1, 5000, size=NUM_ROWS),
    'main_rubric_id': np.random.randint(1, 100, size=NUM_ROWS),
    'content_is_longread': np.random.choice([0, 1], size=NUM_ROWS, p=[0.9, 0.1]),
    'content_editor_id': np.random.randint(1, 1000, size=NUM_ROWS),
    'content_author_ids': [random_string(5) for _ in range(NUM_ROWS)],
    'is_registration': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.45, 0.45, 0.1]),
    'is_fast_login': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.45, 0.45, 0.1]),
    'ua_device_family': [random.choice(['iPhone', 'Android', 'Windows', 'Mac']) for _ in range(NUM_ROWS)],
    'ua_device_brand': [random.choice(['Apple', 'Samsung', 'Xiaomi']) for _ in range(NUM_ROWS)],
    'ua_device_model': [random_string(6) for _ in range(NUM_ROWS)],
    'ua_os_family': [random.choice(['iOS', 'Android', 'Windows', 'MacOS']) for _ in range(NUM_ROWS)],
    'ua_os_version': [str(random.randint(1, 15)) for _ in range(NUM_ROWS)],
    'ua_browser_family': [random.choice(['Chrome', 'Firefox', 'Safari']) for _ in range(NUM_ROWS)],
    'ua_browser_version': [str(random.randint(50, 100)) for _ in range(NUM_ROWS)],
    'ua_is_mobile': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.4, 0.4, 0.2]),
    'ua_is_tablet': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.8, 0.1, 0.1]),
    'ua_is_pc': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.8, 0.1, 0.1]),
    'ua_is_bot': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.9, 0.05, 0.05]),
    'ua_device_type': [random.choice(['mobile', 'tablet', 'desktop']) for _ in range(NUM_ROWS)],
    'geo_city_id': np.random.choice([random.randint(10000, 99999), np.nan], size=NUM_ROWS),
    'date': [START_DATE.date() for _ in range(NUM_ROWS)]
})

# Добавим гарантированные аномалии для page_view
page_view_rows = df[df['event'] == 'page_view'].copy()
anomaly_counter = 0

# Группируем по session_id и user_agent_id и сортируем
grouped = page_view_rows.groupby(['randPAS_user_agent_id', 'randPAS_session_id'])

for (user, sess), group in grouped:
    group = group.sort_values('ts')
    indices = group.index.tolist()
    if len(indices) >= 5 and anomaly_counter < 50:
        # Skip anomaly
        df.loc[indices[3], 'page_view_order_number'] += 3
        # Reset anomaly
        df.loc[indices[4], 'page_view_order_number'] = 1
        anomaly_counter += 1

print(f"✅ Добавлено {anomaly_counter} аномальных сессий для page_view.")

# === Вставка node_id_check аномалий ===
for i in range(5000, 5050):
    df.loc[i, 'node_id'] = np.nan
    df.loc[i, 'main_rubric_id'] = 10
    df.loc[i, 'content_is_longread'] = 1

# === Вставка spikes ===
for i in range(10000, 10100):
    df.loc[i, 'event'] = 'page_view'

# === Вставка скрытых ботов ===
hidden_bot_ips = df['ip'].unique()[:10]
for ip in hidden_bot_ips:
    indices = df[df['ip'] == ip].index
    df.loc[indices, 'ua_is_bot'] = 0  # все 0 — выглядит как человек
    df.loc[indices, 'event'] = 'page_view'

# === Ночная активность ===
df.loc[df['ts'].dt.hour.isin([2, 3, 4]), 'ua_is_bot'] = 1

# === Сохраняем результат ===
os.makedirs("data", exist_ok=True)
df.to_parquet("data/test_activity.parquet", index=False)

print(f"✅ Сгенерировано {NUM_ROWS:,} строк.\nФайл: data/test_activity.parquet")