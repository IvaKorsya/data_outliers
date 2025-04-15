# create_test_data.py
import pandas as pd
import numpy as np

# Тестовые данные
df = pd.DataFrame({
    'timestamp': pd.date_range('2023-01-01', periods=100, freq='H'),
    'value': np.random.randn(100),
    'event': ['page_view']*100
})

# Сохраните в Parquet и CSV
df.to_parquet('data/test_data.parquet')
df.to_csv('data/test_data.csv', index=False)
print("Test data created successfully!")
