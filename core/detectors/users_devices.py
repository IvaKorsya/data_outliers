# core/detectors/users_devices.py
from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class UsersDevicesDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.required_columns = self.config.get(
            'required_columns',
            ['ts', 'ua_is_tablet', 'ua_is_mobile', 'ua_is_pc', 'randPAS_session_id']
        )

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        df = self.load_data(data)
        df = self.identify_anomalies(df)  # Определение многоустройственных пользователей
        self.results = self.analyze_device_usage(df)
        return self.results

    def load_data(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            df = data.copy()
            df['ts'] = pd.to_datetime(df['ts'])
            df['date'] = df['ts'].dt.date
            df['hour'] = df['ts'].dt.hour
            # Заполнение пропущенных значений для столбцов устройств
            for col in ['ua_is_tablet', 'ua_is_mobile', 'ua_is_pc']:
                if col in df.columns:
                    df[col] = df[col].fillna(False)
                else:
                    df[col] = False

            # Обработка столбца randPAS_session_id
            if 'randPASS_session_id' in df.columns:
                df = df.rename(columns={'randPASS_session_id': 'randPAS_session_id'})
            elif 'randPAS_session_id' not in df.columns:
                df['randPAS_session_id'] = None

            # Проверка необходимых столбцов
            required_columns = ['randPAS_session_id', 'ua_is_tablet', 'ua_is_pc', 'ua_is_mobile']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                error_msg = f"Missing required columns: {', '.join(missing_columns)}"
                self.logger.warning(error_msg)
                raise ValueError(error_msg)

            return df
        
        except Exception as e:
            self.logger.error(f"Detection failed: {str(e)}")
            raise


    def identify_anomalies(self, df):
        """Определение пользователей, использующих несколько устройств в рамках одной сессии."""
        session_stats = df.groupby('randPAS_session_id')[['ua_is_tablet', 'ua_is_mobile', 'ua_is_pc']].agg(
            device_types=('ua_is_tablet', lambda x: (x.sum() > 0) + (df.loc[x.index, 'ua_is_mobile'].sum() > 0) + (df.loc[x.index, 'ua_is_pc'].sum() > 0)),
            has_null=('ua_is_tablet', lambda x: ((x == 0) & (df.loc[x.index, 'ua_is_mobile'] == 0) & (df.loc[x.index, 'ua_is_pc'] == 0)).any())
        )
        
        # 2. Добавляем флаги в исходный DataFrame
        df['is_multi_device_session'] = df['randPAS_session_id'].isin(
            session_stats[session_stats['device_types'] > 1].index
        )
        df['is_null_device_session'] = df['randPAS_session_id'].isin(
            session_stats[session_stats['has_null']].index
        )
        
        return df     
        
        

    def analyze_device_usage(self, df):
        """Корректный анализ использования устройств."""
        # 1. Маска одновременного использования устройств
        simultaneous_device_mask = (df[['ua_is_tablet', 'ua_is_pc', 'ua_is_mobile']].sum(axis=1) > 1)
        
        # 2. Подсчёт сессий по типам 
        if 'is_multi_device_session' in df.columns and 'is_null_device_session' in df.columns:
            multi_device_sessions = df['is_multi_device_session'].sum()
            null_device_sessions = df['is_null_device_session'].sum()
            single_device_sessions = len(df['randPAS_session_id'].unique()) - multi_device_sessions - null_device_sessions
        else:
            multi_device_sessions = null_device_sessions = single_device_sessions = 0
        
        # 3. Анализ по пользователям
        device_stats = df.groupby('randPAS_session_id')[['ua_is_tablet', 'ua_is_mobile', 'ua_is_pc']].agg(
            device_types=('ua_is_tablet', lambda x: (x.sum() > 0) + (df.loc[x.index, 'ua_is_mobile'].sum() > 0) + (df.loc[x.index, 'ua_is_pc'].sum() > 0)),
            has_null=('ua_is_tablet', lambda x: ((x == 0) & (df.loc[x.index, 'ua_is_mobile'] == 0) & (df.loc[x.index, 'ua_is_pc'] == 0)).any())
        )
        
        # 4. Пропорции устройств
        device_proportions = df[['ua_is_tablet', 'ua_is_mobile', 'ua_is_pc']].mean()

        device_proportions['multi-device'] = (device_stats['device_types'] > 1).mean()
        device_proportions['null-device'] = device_stats['has_null'].mean()
        
        # 5. Анализ по часам
        hourly_data = df.groupby(df['ts'].dt.hour)['randPAS_session_id'].nunique()

        return {
            'device_proportions': device_proportions,
            'hourly_activity': hourly_data,
            'multi_device_sessions': multi_device_sessions,
            'null_device_sessions': null_device_sessions,
            'single_device_sessions': single_device_sessions,
            'simultaneous_devices': simultaneous_device_mask.sum(),
            'simultaneous_usage': df[df[['ua_is_tablet', 'ua_is_pc', 'ua_is_mobile']].sum(axis=1) > 1]

        }



    def generate_report(self) -> dict:
        """Генерация отчёта об использовании устройств и аномалиях"""
        # Основные метрики из анализа
        device_proportions = self.results['device_proportions']
        hourly_activity = self.results['hourly_activity']
        multi_device = self.results['multi_device_sessions']
        null_device = self.results['null_device_sessions']
        single_device = self.results['single_device_sessions']
        simultaneous = self.results['simultaneous_devices']
        
        # Подготовка данных для таблиц
        usage_df = pd.DataFrame({
            'Device Type': ['PC', 'Mobile', 'Tablet', 'multi-device', 'null-device'],
            'Proportion': [
                device_proportions['ua_is_pc'],
                device_proportions['ua_is_mobile'],
                device_proportions['ua_is_tablet'],
                device_proportions['multi-device'],
                device_proportions['null-device']
            ]
        })
        
        hourly_df = hourly_activity.reset_index(name='sessions').rename(columns={'ts': 'hour'})

        return {
            "summary": (
                f"Device usage analysis:\n"
                f"- Single device sessions: {single_device}\n"
                f"- Multi-device sessions: {multi_device}\n"
                f"- Null device sessions: {null_device}\n"
                f"- Simultaneous device usage: {simultaneous} rows"
            ),
            "metrics": {
                "total_sessions": single_device + multi_device + null_device,
                "pc_users": float(device_proportions['ua_is_pc']),
                "mobile_users": float(device_proportions['ua_is_mobile']),
                "tablet_users": float(device_proportions['ua_is_tablet']),
                "multi_device_sessions": multi_device,
                "null_device_sessions": null_device,
                "simultaneous_usage": simultaneous,
                "peak_hour": int(hourly_activity.idxmax()),
                "sessions_in_peak": int(hourly_activity.max())
            },
            "tables": {
                "device_usage": usage_df.round(3),
                "hourly_activity": hourly_df,
                "simultaneous_usage": self.results['simultaneous_usage']
            },
            "plots": {
                "device_usage_plot": lambda: self._plot_pie(),
                "hourly_activity_plot": lambda: self._plot_hourly()
            }
        }

    def _plot_pie(self):
        """Круговая диаграмма пропорций устройств"""
        plt.figure(figsize=self.plot_config["figure.figsize"])
        device_proportions = self.results['device_proportions']
        print(device_proportions)
        device_proportions.plot(kind='pie', autopct='%1.1f%%', startangle=140,
                                colors=['skyblue', 'lightgreen', 'lightcoral', 'orange', 'gray'])
        plt.title(f'Пропорции использования устройств', fontsize=14)
        plt.ylabel('')

    def _plot_hourly(self):
        """Столбчатая диаграмма активности пользователей по часам"""
        hourly_data = self.results['hourly_activity']
        hourly_data.plot(kind='bar', color='purple')
        plt.title(f'Активность пользователей по часам ', fontsize=14) 
        plt.xlabel('Час дня')
        plt.ylabel('Количество пользователей')
        plt.xticks(rotation=0)
        plt.tight_layout()

