import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string
import os

# === Параметры генерации ===
NUM_ROWS = 100_000
START_DATE = datetime.now() - timedelta(days=1)

# Функция генерации случайной строки
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# === Генерация основных событий ===
events_distribution = (['page_ping'] * 80_000) + (['page_view'] * 10_000) + (
    random.choices(['page_scroll', 'external_link_click', 'impression_view', 'login_signin',
                    'login_signout', 'pay_start', 'pay_confirm', 'subscription_cancel_view'], k=10_000))
random.shuffle(events_distribution)

# === Базовый датафрейм ===
ts_list = [START_DATE + timedelta(seconds=i*30) for i in range(NUM_ROWS)]
ips = [f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}" for _ in range(NUM_ROWS)]

# Создание базовых колонок
df = pd.DataFrame({
    'ts': ts_list,
    'ip': ips,
    'event': events_distribution,
    'page_view_order_number': np.random.randint(1, 20, size=NUM_ROWS, dtype=np.uint32),
    'event_order_number': np.random.randint(1, 100, size=NUM_ROWS, dtype=np.uint32),
    'secs': np.random.randint(0, 60, size=NUM_ROWS, dtype=np.uint32),
    'url': [f"https://example.com/{random_string(5)}" for _ in range(NUM_ROWS)],
    'referer': [f"https://google.com/{random_string(5)}" for _ in range(NUM_ROWS)],
    'randPAS_user_passport_id': [random_string(10) for _ in range(NUM_ROWS)],
    'randPAS_user_agent_id': [random_string(15) for _ in range(NUM_ROWS)],
    'randPAS_trex_cid': [random_string(8) for _ in range(NUM_ROWS)],
    'randPAS_uma_media_cid': [random_string(8) for _ in range(NUM_ROWS)],
    'randPAS_session_id': [random_string(12) for _ in range(NUM_ROWS)],
    'is_new_page': np.random.choice([0, 1], size=NUM_ROWS, p=[0.8, 0.2]).astype(np.int8),
    'title': [f"Title {random.randint(1, 1000)}" for _ in range(NUM_ROWS)],
    'node_id': np.random.randint(1, 5000, size=NUM_ROWS, dtype=np.uint32),
    'main_rubric_id': np.random.randint(1, 100, size=NUM_ROWS, dtype=np.uint32),
    'content_is_longread': np.random.choice([0, 1], size=NUM_ROWS, p=[0.9, 0.1]).astype(np.int8),
    'content_editor_id': np.random.randint(1, 1000, size=NUM_ROWS, dtype=np.uint32),
    'content_author_ids': [random_string(5) for _ in range(NUM_ROWS)],
    'is_registration': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.45, 0.45, 0.1]),
    'is_fast_login': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.45, 0.45, 0.1]),
    'ua_device_family': [random.choice(['iPhone', 'Android', 'Windows', 'Mac', None]) for _ in range(NUM_ROWS)],
    'ua_device_brand': [random.choice(['Apple', 'Samsung', 'Xiaomi', 'None']) for _ in range(NUM_ROWS)],
    'ua_device_model': [random_string(6) for _ in range(NUM_ROWS)],
    'ua_os_family': [random.choice(['iOS', 'Android', 'Windows', 'MacOS']) for _ in range(NUM_ROWS)],
    'ua_os_version': [str(random.randint(1, 15)) for _ in range(NUM_ROWS)],
    'ua_browser_family': [random.choice(['Chrome', 'Firefox', 'Safari']) for _ in range(NUM_ROWS)],
    'ua_browser_version': [str(random.randint(50, 100)) for _ in range(NUM_ROWS)],
    'ua_is_mobile': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.4, 0.4, 0.2]),
    'ua_is_tablet': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.8, 0.1, 0.1]),
    'ua_is_pc': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.8, 0.1, 0.1]),
    'ua_is_bot': np.random.choice([0, 1, np.nan], size=NUM_ROWS, p=[0.95, 0.04, 0.01]),
    'ua_device_type': [random.choice(['mobile', 'tablet', 'desktop']) for _ in range(NUM_ROWS)],
    'geo_city_id': np.random.choice([random.randint(10000, 99999), np.nan], size=NUM_ROWS),
    'date': [START_DATE.date() for _ in range(NUM_ROWS)]
})

# === Специальные аномалии ===
# === АНОМАЛИИ ДЛЯ page_view ===
# Специально создаём неправильную последовательность просмотров в сессиях
session_ids = df['randPAS_session_id'].dropna().unique()

for sess in np.random.choice(session_ids, size=30, replace=False):
    indices = df[df['randPAS_session_id'] == sess].index
    if len(indices) > 3:
        glitch_idx = np.random.choice(indices[1:], size=1)
        df.loc[glitch_idx, 'page_view_order_number'] += np.random.randint(2, 5)  # пропуск нескольких порядков


# 2. node_id_check аномалии
for i in range(20000, 21000, 100):
    df.loc[i:i+4, 'node_id'] = 0  # Изменено i:i+4 вместо i:i+5

# 3. activity_spikes
for i in range(30000, 40000, 500):
    df.loc[i:i+9, 'event'] = 'page_view'  # Изменено i:i+9 вместо i:i+10

# 4. night_activity (добавляем всплески ночью)
df.loc[df['ts'].dt.hour.isin([2, 3, 4]), 'ua_is_bot'] = 1

# === Сохраняем в Parquet ===
os.makedirs('data', exist_ok=True)
df.to_parquet('data/test_activity.parquet')

print(f"✅ Сгенерировано {NUM_ROWS:,} строк с аномалиями.\nФайл сохранен: data/test_activity.parquet")