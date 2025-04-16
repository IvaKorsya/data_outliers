# core/detectors/activity_spikes.py
import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
from core.base_detector import BaseAnomalyDetector
import matplotlib.pyplot as plt
import logging

class ActivitySpikesDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.schedule_file = config.get('schedule_file')
        self.logger = logging.getLogger(__name__)
        
    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        """Основной метод обнаружения аномалий"""
        try:
            # Фильтрация и обработка данных
            data = data[data['event'] == 'page_view'].copy()
            data['ts'] = pd.to_datetime(data['ts'])
            
            # Агрегация по минутам
            time_res = self.config.get('time_resolution', '1min')
            activity = data.groupby(data['ts'].dt.floor(time_res)).size().reset_index(name='requests')
            
            # Поиск пиков
            window_size = self.config.get('window_size', 10)
            peaks_idx = argrelextrema(activity['requests'].values, np.greater, order=window_size)[0]
            activity['is_peak'] = activity.index.isin(peaks_idx)
            
            self.peaks = activity[activity['is_peak']].nlargest(
                self.config.get('top_n', 10), 
                'requests'
            )
            
            if self.schedule_file:
                self._match_with_schedule()
                
            self.results = activity  # Сохраняем для использования в отчете
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
                ][['title', 'event_type', 'channel_id']].to_dict('records')
        except Exception as e:
            self.logger.warning(f"Schedule matching failed: {str(e)}")

    def generate_report(self) -> dict:
        """Генерация стандартизированного отчета"""
        if not hasattr(self, 'peaks'):
            raise ValueError("No peaks detected - run detect() first")
            
        report = {
            "summary": f"Found {len(self.peaks)} activity spikes (min: {self.peaks['requests'].min()}, max: {self.peaks['requests'].max()})",
            "metrics": {
                "total_spikes": len(self.peaks),
                "max_requests": self.peaks['requests'].max(),
                "min_requests": self.peaks['requests'].min(),
                "avg_requests": self.peaks['requests'].mean()
            },
            "tables": {
                "peaks_data": self.peaks,
                "full_activity": self.results
            },
            "plots": {
                "activity_plot": self._plot_activity
            }
        }
        
        if 'matched_shows' in self.peaks:
            report["metrics"]["matched_shows"] = sum(self.peaks['matched_shows'].astype(bool))
            
        return report

    def _plot_activity(self):
        """Генерация графика активности"""
        plt.figure(figsize=(14, 6))
        plt.plot(self.results['ts'], self.results['requests'], 
               label='Requests', color='blue', alpha=0.7)
        plt.scatter(self.peaks['ts'], self.peaks['requests'], 
                  color='red', label='Spikes', zorder=3)
        plt.xlabel('Time')
        plt.ylabel('Requests count')
        plt.title(f'Activity Spikes (Top {len(self.peaks)})')
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
