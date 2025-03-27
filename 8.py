import pandas as pd

# Считываем датасет из файла
file_path = 'data_2024-10-09.parquet'
data = pd.read_parquet(file_path)

# Условия для проверки
required_columns = ['url', 'main_rubric_id', 'content_is_longread', 'content_editor_id', 'content_author_ids', 'title']

# Фильтрация строк, где node_id отсутствует и хотя бы один из требуемых столбцов заполнен
missing_node_id = data[data['node_id'].isnull() & data[required_columns].notnull().any(axis=1)]

# Проверяем результаты
if not missing_node_id.empty:
    print("Найдены строки с отсутствующим node_id и заполненными столбцами:")
    print(missing_node_id)
else:
    print("Нет строк с отсутствующим node_id и заполненными столбцами.")
