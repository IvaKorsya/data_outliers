import pandas as pd
import numpy as np
import glob
import matplotlib.pyplot as plt
from scipy.signal import argrelextrema
import os
from google.colab import drive


def analyze_data(dataset_path, schedule_file):

    # 1. Загружаем все паркет-файлы и объединяем
    all_files = glob.glob(dataset_path)
    if not all_files:
        raise FileNotFoundError(f"Не найдены файлы по указанному пути: {dataset_path}")
    # Чтение файлов (только нужные столбцы) с фильтрацией (только `page_view`)
    chunks = []
    for f in all_files:
        chunk = pd.read_parquet(f, columns=['event', 'ts'])
        chunk = chunk[chunk['event'] == 'page_view']
        chunks.append(chunk)

    data = pd.concat(chunks)
    del chunks  # Освобождаем память

    # 2. Преобразуем `ts` в datetime и группируем по минутам
    data['ts'] = pd.to_datetime(data['ts'])
    activity = data.groupby(data['ts'].dt.floor('min')).size().reset_index(name='requests')

    # 3. Ищем локальные максимумы (топ-10 всплесков)
    activity['local_max'] = activity.iloc[argrelextrema(activity['requests'].values, np.greater, order=10)[0]]['requests']
    peaks = activity.nlargest(10, 'requests')

    # 4. Загружаем телепрограмму и рассчитываем `end_ts`
    if not os.path.exists(schedule_file):
        raise FileNotFoundError(f"Файл телепрограммы не найден: {schedule_file}")
    schedule_df = pd.read_csv(schedule_file)
    schedule_df['start_ts'] = pd.to_datetime(schedule_df['start_ts'])
    schedule_df['end_ts'] = schedule_df['start_ts'] + pd.to_timedelta(schedule_df['dur'], unit='s')

    # 5. Сопоставляем всплески с передачами
    def find_show(timestamp):
        show = schedule_df[(schedule_df['start_ts'] <= timestamp) & (schedule_df['end_ts'] >= timestamp)]
        return show[['title', 'event_type', 'channel_id']].to_dict(orient='records')

    peaks['matched_shows'] = peaks['ts'].apply(find_show)

    def rating_programs(schedule_file, top_k=10):
      # Загружаем телепрограмму, нам понадобится только одно поле 'event_type'
      if not os.path.exists(schedule_file):
          raise FileNotFoundError(f"Файл телепрограммы не найден: {schedule_file}")
      cols = ['event_type']
      schedule_df = pd.read_csv(schedule_file, usecols = cols)

      # Считаем количество повторений типов передач. Каких передач было больше всего-те самые популярные.
      counts = schedule_df[schedule_df['event_type'] != 'Прочее']['event_type'].value_counts()

      # Получаем топ-k
      top = counts.head(top_k)
      print("\nТоп-10 самых популярных программ:\n")
      print(top.to_string())
      return top

    top_k = 10; # сколько мест в рейтинге
    top = rating_programs(schedule_file, top_k)

    # 6. Визуализация
    plt.figure(figsize=(14, 6))
    plt.plot(activity['ts'], activity['requests'], label='Все запросы', color='blue', alpha=0.7)
    plt.scatter(peaks['ts'], peaks['requests'], color='red', label='Топ-10 всплесков', zorder=3)
    plt.xlabel('Время')
    plt.ylabel('Количество запросов')
    plt.title('Всплески активности и их соответствие передачам')
    plt.legend()
    plt.xticks(rotation=45)
    plt.grid()
    plt.show()

    # 7. Улучшенный вывод
    print("\n=== Всплески активности и соответствующие передачи ===\n")
    for index, row in peaks.iterrows():
        print(f"📌 Время всплеска: {row['ts']}")
        print(f"   🔺 Запросов: {row['requests']}")
        if row['matched_shows']:

            found_top_show = False  # Флаг для отслеживания найденных популярных передач

            for show in row['matched_shows']:
                if show['event_type'] in top:
                    print(f"   ✅ Оправданный всплеск. Шла популярная передача: {show['title']} (Тип: {show['event_type']}, Канал: {show['channel_id']})")
                    found_top_show = True
                    break

            if not found_top_show:
                print("   ⚠️ ATTENTION: Всплеск активности, но не совпадает с топовыми передачами")
                print("   Передачи в это время:")
                for show in row['matched_shows']:
                    print(f"      - {show['title']} (Тип: {show['event_type']}, Канал: {show['channel_id']})")
        else:
            print("   ❌ Нет совпадений с телепрограммой.")
        print("-" * 50)



# Монтируем Google Drive и загружаем данные
drive.mount('/content/drive')

# Запрос путей у пользователя (для Colab)
dataset_path = input("Введите путь к паркет-файлам (например, /content/drive/MyDrive/dataset/*.parquet): ").strip()
schedule_file = input("Введите путь к файлу телепрограммы (например, /content/drive/MyDrive/aggrs_tv_program_epg_plan.csv): ").strip()

# Запуск анализа
analyze_data(dataset_path, schedule_file)
