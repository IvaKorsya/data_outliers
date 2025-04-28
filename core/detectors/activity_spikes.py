# core/detectors/activity_spikes.py
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import argrelextrema
from core.base_detector import BaseAnomalyDetector

class ActivitySpikesDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.schedule_file = self.config.get('schedule_file')
        self.time_resolution = self.config.get('time_resolution', '5min')
        self.window_size = self.config.get('window_size', 10)
        self.top_n = self.config.get('top_n', 10)

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        self.logger.debug(f"[DEBUG] Input rows: {len(data)}")
        if 'event' not in data.columns or 'ts' not in data.columns:
            raise ValueError("Required columns 'event' and 'ts' are missing.")

        event_counts = data['event'].value_counts().to_dict()
        self.logger.debug(f"[DEBUG] Events: {event_counts}")

        data = data[data['event'] == 'page_view']
        if data.empty:
            raise ValueError("No 'page_view' events found.")

        data['ts'] = pd.to_datetime(data['ts'])
        data['ua_is_bot'] = data.get('ua_is_bot', 0).fillna(0).astype(int)
        data['is_bot'] = data['ua_is_bot'] > 0

        activity = (
            data.groupby(data['ts'].dt.floor(self.time_resolution))
                .size()
                .reset_index(name='total_requests')
        )

        self.logger.debug(f"[DEBUG] Activity shape: {activity.shape}")

        if len(activity) <= self.window_size * 2:
            self.logger.warning("Not enough data points to detect spikes.")
            self.peaks = pd.DataFrame()
            self.results = activity
            return activity

        activity['is_peak'] = False
        peaks_idx = argrelextrema(activity['total_requests'].values, np.greater, order=self.window_size)[0]
        activity.loc[peaks_idx, 'is_peak'] = True
        self.peaks = activity[activity['is_peak']].nlargest(self.top_n, 'total_requests').copy()

        if self.schedule_file:
            self._match_with_schedule()
        else:
            self.peaks['matched_shows'] = [[] for _ in range(len(self.peaks))]
            self.peaks['matched_count'] = 0

        self.results = activity
        return activity

    def _match_with_schedule(self):
        try:
            schedule_df = pd.read_csv(self.schedule_file)
            schedule_df['start_ts'] = pd.to_datetime(schedule_df['start_ts'])
            schedule_df['end_ts'] = schedule_df['start_ts'] + pd.to_timedelta(schedule_df['dur'], unit='s')

            def find_matches(ts):
                matched = schedule_df[(schedule_df['start_ts'] <= ts) & (schedule_df['end_ts'] >= ts)]
                return matched[['title', 'event_type', 'channel_id']].to_dict(orient='records')

            self.peaks['matched_shows'] = self.peaks['ts'].apply(find_matches)
            self.peaks['matched_count'] = self.peaks['matched_shows'].apply(len)

        except Exception as e:
            self.logger.warning(f"Schedule matching failed: {e}")
            self.peaks['matched_shows'] = [[] for _ in range(len(self.peaks))]
            self.peaks['matched_count'] = 0

    def generate_report(self) -> dict:
        if self.peaks.empty:
            return {
                "summary": "No activity spikes detected.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        matched_total = int(self.peaks['matched_count'].sum()) if 'matched_count' in self.peaks else 0
        unmatched_total = int((self.peaks['matched_count'] == 0).sum())

        return {
            "summary": f"Found {len(self.peaks)} activity spikes.",
            "metrics": {
                "total_spikes": int(len(self.peaks)),
                "matched_shows": matched_total,
                "unmatched_spikes": unmatched_total,
                "max_requests": int(self.peaks['total_requests'].max()),
                "min_requests": int(self.peaks['total_requests'].min()),
                "avg_requests": float(self.peaks['total_requests'].mean())
            },
            "tables": {
                "top_spikes": self.peaks
            },
            "plots": {
                "activity_spikes_plot": self._plot_activity
            }
        }

    def _plot_activity(self):
        if self.results is None or self.results.empty:
            return

        plt.figure(figsize=(12, 6))
        plt.plot(self.results['ts'], self.results['total_requests'], label='Total Requests', alpha=0.7)
        if not self.peaks.empty:
            plt.scatter(self.peaks['ts'], self.peaks['total_requests'], color='red', label='Spikes')
        plt.title("Activity Spikes")
        plt.xlabel("Time")
        plt.ylabel("Requests")
        plt.xticks(rotation=45)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
