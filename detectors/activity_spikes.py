# detectors/activity_spikes.py
import pandas as pd
import numpy as np
import glob
import matplotlib.pyplot as plt
from scipy.signal import argrelextrema
import os
from pathlib import Path
from core.base_detector import BaseAnomalyDetector

class ActivitySpikesDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.schedule_file = config.get('schedule_file')
        
    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        """Основной метод обнаружения аномалий"""
        # 1. Фильтрация только page_view событий
        data = data[data['event'] == 'page_view']
        data['ts'] = pd.to_datetime(data['ts'])
        
        # 2. Агрегация по минутам
        activity = data.groupby(data['ts'].dt.floor(self.config.get('time_resolution', '1min'))).size().reset_index(name='requests')
        
        # 3. Поиск пиков
        activity['local_max'] = activity.iloc[
            argrelextrema(activity['requests'].values, np.greater, 
                         order=self.config.get('window_size', 10))[0]
        ]['requests']
        
        self.peaks = activity.nlargest(self.config.get('top_n', 10), 'requests')
        
        if self.schedule_file:
            self._match_with_schedule()
            
        return activity
    
    def _match_with_schedule(self):
        """Сопоставление с телепрограммой"""
        schedule_df = pd.read_csv(self.schedule_file)
        schedule_df['start_ts'] = pd.to_datetime(schedule_df['start_ts'])
        schedule_df['end_ts'] = schedule_df['start_ts'] + pd.to_timedelta(schedule_df['dur'], unit='s')
        
        def find_show(timestamp):
            show = schedule_df[(schedule_df['start_ts'] <= timestamp) & 
                             (schedule_df['end_ts'] >= timestamp)]
            return show[['title', 'event_type', 'channel_id']].to_dict(orient='records')
        
        self.peaks['matched_shows'] = self.peaks['ts'].apply(find_show)
    
    def generate_report(self) -> dict:
        """Генерация стандартизированного отчета"""
        report = {
            "summary": f"Found {len(self.peaks)} activity spikes",
            "metrics": {
                "max_requests": self.peaks['requests'].max(),
                "min_requests": self.peaks['requests'].min(),
                "avg_requests": self.peaks['requests'].mean()
            },
            "tables": {
                "top_spikes": self.peaks
            },
            "plots": {
                "activity_plot": self._plot_activity
            }
        }
        
        if hasattr(self, 'peaks') and 'matched_shows' in self.peaks:
            report["metrics"]["matched_shows"] = len(
                [x for x in self.peaks['matched_shows'] if x]
            )
        
        return report
    
    def _plot_activity(self):
        """Генерация графика активности"""
        plt.figure(figsize=(14, 6))
        plt.plot(self.results['ts'], self.results['requests'], 
                label='All requests', color='blue', alpha=0.7)
        plt.scatter(self.peaks['ts'], self.peaks['requests'], 
                  color='red', label='Top spikes', zorder=3)
        plt.xlabel('Time')
        plt.ylabel('Requests count')
        plt.title('Activity spikes')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid()
# Запуск анализа
analyze_data(dataset_path, schedule_file)
