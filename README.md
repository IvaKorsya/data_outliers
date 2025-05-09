# Фреймворк для обнаружения аномалий в пользовательской активности

Данный фреймворк предназначен для автоматического поиска аномалий в пользовательских логах. Он модульный, расширяемый, включает множество детекторов, поддержку отчётности, генерацию тестовых данных и гибкую CLI-интерфейсную оболочку.

# Аномалии
— это отклонения от нормального поведения пользователей или систем. В контексте веб-активности, это может быть:

- необычный всплеск трафика (всплеск активности);

- пробелы в данных (например, отсутствующий node_id);

- "скачки" в номерах просмотров (page_view);

- подозрительные пользователи/боты;

- поведение, отличающееся от привычного — например, ночная активность, множественные устройства, и т.п.

# Цель анализа
— найти такие случаи, интерпретировать их и принять меры:

-  отфильтровать подозрительные данные;

-  провести ручную проверку контента;

-  настроить алерты;

-  улучшить систему сбора или валидации данных.



# Включённые детекторы и их принцип работы

| Детектор             | Что ищет    | Метод выявления |
|----------------------|-----------|-------------|
| `activity_spikes`    |  Временные всплески активности | 	Агрегация по времени, локальные максимумы, сопоставление с телепрограммой |
| `node_id_check`      |  Контент без node_id, но с признаками описания | Проверка node_id == NaN, при наличии title, main_rubric_id и пр. |
| `untagged_bots`      |  Боты, которые не размечены как ua_is_bot == 1 и их активность | Анализ заголовков ua_header, частоты, сравнение с известными ботами |
| `night_activity`     |  Активность в нетипичное время | Находит активность в ночное время и сравнивает с дневной:Запросы между 0:00–6:00, особенно IP с высокой активностью |
| `page_view`          |  Пропущенные или сброшенные номера просмотро | Анализ последовательности page_view_order_number |
| `isolation_forest`   |  Нетипичные временные отрезки | 	Машинное обучение: обучение на requests, unique_ips, bot_ratio |
| `users_devices`      |  Сессии с множеством или отсутствием типов устройств | Анализ признаков ua_is_mobile, ua_is_pc, ua_is_tablet |

# Как интерпретировать и использовать отчёты?

activity_spikes
→ Сравните со временем трансляции. Если совпадает с ТВ-передачей — можно объяснить. Если нет — возможно, спам или накрутка.

node_id_check
→ Удалите записи без node_id, но с описанием контента — они могут быть невалидными.

untagged_bots
→ Проверьте ua_header. Если это Googlebot и т.п., можно обновить правила. Если подозрительные — блокируйте IP или уточните правила фильтрации.

page_view
→ Частые reset могут означать сбои в счётчике, skip — потери событий. Анализируйте по сессиям.

users_devices
→  Одновременные несколько типов устройств у одного пользователя - аномалия, следует проверить такие сессии. Также важно выявлять аномалии типа "нет устройства".

# Как адаптировать или расширить анализ?
- Используйте детекторы выборочно — на разных выборках и по частям.

- Чтобы настроить поведение фреймворка под задачи конкретного анализа, можно изменять параметры в core/config/local.yaml. Вот примеры изменений и их влияние:

activity_spikes

> time_resolution — размер временного окна агрегации.
Пример: "1H" (по часам), "5min" — повышает чувствительность.

> window_size — насколько "широко" искать локальные пики.
Пример: 5 → анализируется окно из 10 ближайших интервалов.

> top_n — сколько наибольших пиков включить в отчёт.
Увеличьте, если хотите видеть больше аномалий.
```
activity_spikes:
  enabled: true
  time_resolution: "5min"
  window_size: 10
  top_n: 20
```
isolation_forest

>contamination — какая доля данных считается аномалиями.
Пример: 0.02 = 2% самых "нетипичных" интервалов.

>interval_minutes — ширина временного окна агрегации.
Пример: 3 = агрегировать каждые 3 минуты.

>n_estimators — количество деревьев в модели.
Больше → стабильнее, но медленнее.
```
isolation_forest:
  enabled: true
  contamination: 0.02
  n_estimators: 150
  interval_minutes: 3
```
node_id_check

>validate_hourly — если true, ищет конфликты в разрезе часов.
Помогает отследить временные сбои.

>required_columns — какие признаки указывают на "контент".
Пример: ['url', 'node_id'] или добавить main_rubric_id, title, и т.д.
```
node_id_check:
  enabled: true
  required_columns: ['url', 'node_id', 'title']
  validate_hourly: true
```
untagged_bots
>top_n — сколько наиболее подозрительных агентов или IP включить в отчёт.
Можно увеличить при низком уровне шума в выборке.
```
untagged_bots:
  enabled: true
  top_n: 10
```

users_devices
>device_columns — какие признаки считать устройствами.

>session_id_column — как агрегировать по сессиям.
```users_devices:
  enabled: true
  time_column: 'ts'
  session_id_column: 'randPAS_session_id'
  device_columns: ['ua_is_pc', 'ua_is_mobile', 'ua_is_tablet']
```
night_activity
>min_requests_threshold — сколько запросов от IP ночью нужно, чтобы считать его подозрительным.
```
night_activity:
  enabled: true
  min_requests_threshold: 100
```
- Пишите свои детекторы: достаточно унаследоваться от BaseAnomalyDetector.

##  Как установить зависимости

```bash
pip install -r requirements.txt
```
##  Советы для доработки детекторов

- Всегда наследуйся от `BaseAnomalyDetector`
- В `detect()` — вся логика и фильтрация
- В `generate_report()` — генерация таблиц, графиков, метрик
- В отчёте конвертируй всё в нативные Python-типажи: `int`, `float`, `str`
- Сначала сам сгенерируй аномалию в синкретических данных, проведи запуск и удостоверься, что находит,  потом уже ищи в датасете
# Как запустить анализ?

## 🚀 Как запустить анализ реальных данных

```bash
python main.py \
  --config core/config/local.yaml \
  --data-path data/test_activity.parquet \
  --detectors activity_spikes node_id_check \
  --output-format html \
  --output-dir outputs/reports/manual_run \
  --log-level DEBUG
```
# Как сгенерировать тестовые данные?
```
python test_data.py
```
Сгенерируется файл data/test_activity.parquet со 100k строк и следующими аномалиями:

-Всплески activity_spikes

-Пропущенные node_id

-Сбои в page_view_order_number

-Боты без ua_is_bot

-Смешанное использование устройств users_devicess

Пример тестового запуска:
```
python main.py \
  --config core/config/local.yaml \
  --data-path data/test_activity.parquet \
  --detectors activity_spikes,node_id_check,page_view \
  --output-format html \
  --log-level INFO
```
Можно использовать --detectors all, если нужно запустить всё, что включено в конфиг (enabled: true).



# Как переделать нетипичный метод в детектор, совместимый с фреймворком

Шаги по адаптации:

 1. Создай файл core/detectors/your_method.py

 2. Создай класс, унаследованный от BaseAnomalyDetector, пример брать с самого базового детектора, найти его можно в ядре

 3. Перенеси ключевую логику:

Из скрипта:

- фильтрация → в detect()

- сохранение → заменить на self.results = ...

- графики → сделать функциями и вернуть в generate_report

Пример — до и после

# Было:
```
df = pd.read_parquet("data.parquet")
peaks = detect_spikes(df)
peaks.to_csv("output.csv")
```
# Стало:
```
def detect(self, data):
    ...
    self.results = peaks_df
    return data
```
4.Добавь импорт в core/detectors/__init__.py:
```
from .your_method import YourMethodDetector
__all__ = [...
           ('YourMethodDetector'),
          ....]
```
5.Добавь в main.py:
```
from core.detectors.your_method import YourMethodDetector

detectors = {
    ...
    'your_method': YourMethodDetector
}
```
6.Проверь конфигурацию в core/config/local.yaml:
```
  your_method:
    enabled: true
    threshold: 0.9
    ...
```
7. Допиши test_data.py, если нужны специальные данные(а они чаще всего нужны)

#  Структура generate_report() должна возвращать(как минимум):
```
{
  "summary": "Краткое описание результата",
  "metrics": {"метрика": значение},
  "tables": {"название": pd.DataFrame},
  "plots": {"название": функция_отрисовки}
}
```
# Как фреймворк работает внутри?

1. main.py запускает AnalysisRunner

2. Загружается дата через DataLoader

3. Для каждого детектора:

    3.1 вызывается detect()

    3.2 затем generate_report()

    3.3 сохраняется в outputs/reports/<название_детектора>/

 4. report_generator.py собирает финальный summary_report.html или .json

#  Пример финального отчёта:

- meta.json — краткое описание и параметры

- tables/*.parquet|csv — сохранённые таблицы

- plots/*.png — графики

- summary_report.html — сводный HTML отчёт

 # Cтруктуры фреймворка

```
framework_development/
│
├── main.py                    # Точка входа
├── test_data.py               # Генерация тестовых данных
├── requirements.txt
│
├── core/                           # Ядро системы
    ├── config/                        # Конфигурации для всех сред
│      ├── default.yaml                # Настройки для Google Colab
│      ├── local.yaml                  # Локальные настройки
│      └── production.yaml             # Продакшен-конфиг
    ├── detectors/              
│      ├── __init__.py
│      ├── activity_spikes.py          # Анализ всплесков активности
│      ├── isolation_forest.py         # Isolation Forest
│      ├── night_activity.py           # Ночная активность
│      ├── node_id_check.py            # Проверка node_id
│      ├── page_view.py                # Аномалии просмотров
│      └── untagged_bots.py            # Неотмеченные боты
│
│   ├── base_detector.py            # Базовый класс детектора
│   ├── config_manager.py           # Загрузка конфигов
│   ├── data_loader.py              # Умный загрузчик данных
│   ├── report_generator.py         # Генерация отчетов
│   └── runner.py                   # Координатор
│
├── data/                           # Входные данные (test/реальные)
├── outputs/                        # Автосохранение результатов
│   ├── reports/                    # Готовые отчеты
└── README.md                       # Инструкции
```
