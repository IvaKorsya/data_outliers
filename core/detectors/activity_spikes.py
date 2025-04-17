# core/detectors/activity_spikes.py
from core.base_detector import BaseAnomalyDetector
import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
import logging
import matplotlib.pyplot as plt

class ActivitySpikesDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.schedule_file = self.config.get('schedule_file')
        self.time_resolution = self.config.get('time_resolution', '5min')
        self.window_size = self.config.get('window_size', 10)
        self.top_n = self.config.get('top_n', 10)
        
    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        """Основной метод обнаружения аномалий"""
        try:
            # Фильтрация данных
            data = self._filter_data(data, {'event': 'page_view'})
            data['ts'] = pd.to_datetime(data['ts'])
            
            # Агрегация активности по временным интервалам
            activity = data.groupby(data['ts'].dt.floor(self.time_resolution))['requests'].sum()
            activity = activity.reset_index(name='total_requests')
            
            # Поиск пиков
            peaks_idx = argrelextrema(
                activity['total_requests'].values, 
                np.greater, 
                order=self.window_size
            )[0]
            
            activity['is_peak'] = activity.index.isin(peaks_idx)
            self.peaks = activity[activity['is_peak']].nlargest(self.top_n, 'total_requests')
            
            if self.schedule_file:
                self._match_with_schedule()
                
            self.results = activity
            return activity
            
        except Exception as e:
            self.logger.error(f"Detection failed: {str(e)}")
            raise

    def _match_with_schedule(self):
        """Сопоставление пиков с телепрограммой"""
        try:
            schedule_df = pd.read_csv(self.schedule_file)
            schedule_df['start_ts'] = pd.to_datetime(schedule_df['start_ts'])
            schedule_df['end_ts'] = schedule_df['start_ts'] + pd.to_timedelta(schedule_df['dur'], unit='s')
            
            self.peaks['matched_shows'] = self.peaks['ts'].apply(
                lambda x: schedule_df[
                    (schedule_df['start_ts'] <= x) & 
                    (schedule_df['end_ts'] >= x)
                ][['title', 'event_type', 'channel_id']].to_dict('records'))
        except Exception as e:
            self.logger.warning(f"Schedule matching failed: {str(e)}")

    def generate_report(self) -> dict:
        """Генерация стандартизированного отчета"""
        if not hasattr(self, 'peaks') or self.peaks.empty:
            return {
                "summary": "No activity spikes detected",
                "metrics": {},
                "tables": {},
                "plots": {}
            }
        
        # Конвертируем numpy типы в нативные Python типы
        peaks_df = self.peaks.copy()
        peaks_df['total_requests'] = peaks_df['total_requests'].astype(int)
        
        return {
            "summary": f"Found {len(self.peaks)} activity spikes (min: {int(self.peaks['total_requests'].min())}, max: {int(self.peaks['total_requests'].max())})",
            "metrics": {
                "total_spikes": int(len(self.peaks)),
                "max_requests": int(self.peaks['total_requests'].max()),
                "min_requests": int(self.peaks['total_requests'].min()),
                "avg_requests": float(self.peaks['total_requests'].mean())
            },
            "tables": {
                "peaks_data": peaks_df,
                "full_activity": self.results
            },
            "plots": {
                "activity_plot": self._plot_activity
            }
        }

    def _plot_activity(self):
        """Генерация графика активности"""
        plt.figure(figsize=self.plot_config["figure.figsize"])
        plt.plot(self.results['ts'], self.results['total_requests'], 
               label='Requests', color='blue', alpha=0.7)
        plt.scatter(self.peaks['ts'], self.peaks['total_requests'], 
                  color='red', label='Spikes', zorder=3)
        plt.title('Activity Spikes Detection')
        plt.xlabel('Time')
        plt.ylabel('Requests count')
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)