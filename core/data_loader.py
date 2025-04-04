import pandas as pd
import glob
import os
from datetime import datetime
from typing import Union, List, Optional

class DataLoader:
    def __init__(self, config=None):
        self.config = config or {}
        
    def load(
        self,
        path: str,
        date_range: Optional[tuple] = None,
        file_mask: str = "*.parquet",
        filters: Optional[dict] = None
    ) -> pd.DataFrame:
        """
        Умная загрузка с поддержкой:
        - Фильтрации по датам
        - Выбора конкретных файлов
        - Дополнительных фильтров
        """
        if os.path.isdir(path):
            return self._load_folder(path, file_mask, date_range, filters)
        return self._load_single(path, filters)
    
    def _load_folder(
        self,
        folder_path: str,
        file_mask: str,
        date_range: Optional[tuple],
        filters: Optional[dict]
    ) -> pd.DataFrame:
        files = self._find_matching_files(folder_path, file_mask, date_range)
        
        dfs = []
        for f in files:
            df = pd.read_parquet(f)
            if filters:
                df = self._apply_filters(df, filters)
            dfs.append(df)
            
        return pd.concat(dfs, ignore_index=True)
    
    def _find_matching_files(
        self,
        folder_path: str,
        file_mask: str,
        date_range: Optional[tuple]
    ) -> List[str]:
        """Поиск файлов с учётом дат в именах"""
        all_files = glob.glob(os.path.join(folder_path, file_mask))
        
        if not date_range:
            return all_files
            
        start_date, end_date = date_range
        matched_files = []
        
        for f in all_files:
            try:
                file_date = self._extract_date_from_filename(f)
                if start_date <= file_date <= end_date:
                    matched_files.append(f)
            except:
                continue
                
        return matched_files
    
    def _extract_date_from_filename(self, filename: str) -> datetime:
        """Парсинг даты из имени файла (например data_2024-10-09.parquet)"""

        pass
    
    def _apply_filters(self, df: pd.DataFrame, filters: dict) -> pd.DataFrame:
        """Применение фильтров к данным"""
        for col, condition in filters.items():
            if col in df.columns:
                if callable(condition):
                    df = df[condition(df[col])]
                else:
                    df = df[df[col] == condition]
        return df
    
    def _load_single(self, file_path: str, filters: Optional[dict]) -> pd.DataFrame:
        df = pd.read_parquet(file_path)
        return self._apply_filters(df, filters) if filters else df
