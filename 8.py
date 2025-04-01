import pandas as pd
import os
import glob
from IPython.display import display
from google.colab import drive
drive.mount('/content/drive')

def get_user_file_path():
    """Запрашивает путь к файлу/папке у пользователя с проверкой"""
    while True:
        path = input("Введите путь к файлу .parquet или папке с файлами: ").strip()
        
        # Проверка существования пути
        if not os.path.exists(path):
            print(f"Ошибка: путь '{path}' не существует. Попробуйте снова.")
            continue
            
        # Если это папка - ищем parquet-файлы
        if os.path.isdir(path):
            parquet_files = glob.glob(os.path.join(path, '*.parquet'))
            if not parquet_files:
                print(f"В папке '{path}' не найдено .parquet файлов.")
                continue
            return parquet_files[0] if len(parquet_files) == 1 else path
            
        # Если это файл - проверяем расширение
        elif not path.lower().endswith('.parquet'):
            print("Файл должен иметь расширение .parquet")
            continue
            
        return path

def load_data(file_path):
    """Загружает данные с обработкой ошибок"""
    try:
        if os.path.isdir(file_path):
            # Загрузка всех parquet-файлов из папки
            files = glob.glob(os.path.join(file_path, '*.parquet'))
            dfs = [pd.read_parquet(f) for f in files]
            data = pd.concat(dfs, ignore_index=True)
            print(f"Загружено {len(files)} файлов, всего {len(data)} строк")
        else:
            # Загрузка одного файла
            data = pd.read_parquet(file_path)
            print(f"Загружен файл {os.path.basename(file_path)}, {len(data)} строк")
        return data
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        return None

def analyze_missing_node_ids(data):
    """Анализирует строки с отсутствующим node_id"""
    # Условия для проверки
    required_columns = ['url', 'main_rubric_id', 'content_is_longread', 
                       'content_editor_id', 'content_author_ids', 'title']
    
    # Проверяем наличие всех требуемых столбцов
    missing_cols = [col for col in required_columns if col not in data.columns]
    if missing_cols:
        print(f"Предупреждение: отсутствуют столбцы: {', '.join(missing_cols)}")
        required_columns = [col for col in required_columns if col in data.columns]
    
    # Фильтрация строк
    mask = data['node_id'].isnull() & data[required_columns].notnull().any(axis=1)
    missing_node_id = data[mask]
    
    return missing_node_id, required_columns

def generate_report(missing_data, columns_checked):
    """Генерирует детальный отчет"""
    if not missing_data.empty:
        print("\n" + "="*50)
        print(f"Найдено {len(missing_data)} строк с отсутствующим node_id")
        print(f"Проверяемые столбцы: {', '.join(columns_checked)}")
        print("="*50)
        
        # Группировка по причинам
        reasons = []
        for col in columns_checked:
            reason_count = len(missing_data[missing_data[col].notnull()])
            reasons.append(f"{col}: {reason_count}")
        
        print("\nПричины (заполненные столбцы):")
        print("\n".join(reasons))
        
        # Топ-5 наиболее частых URL
        if 'url' in missing_data.columns:
            print("\nТоп-5 URL с проблемами:")
            display(missing_data['url'].value_counts().head(5))
        
        # Сохранение результатов
        save_path = "missing_node_id_results.csv"
        missing_data.to_csv(save_path, index=False)
        print(f"\nРезультаты сохранены в {save_path}")
    else:
        print("\nПроблемных строк не обнаружено.")

def main():
    print("Анализ отсутствующих node_id")
    
    # Получаем путь к данным
    file_path = get_user_file_path()
    
    # Загружаем данные
    data = load_data(file_path)
    if data is None:
        return
    
    # Анализируем проблемные строки
    missing_data, checked_columns = analyze_missing_node_ids(data)
    
    # Генерируем отчет
    generate_report(missing_data, checked_columns)

if __name__ == "__main__":
    main()
