# core/detectors/night_activity.py
from core.base_detector import BaseAnomalyDetector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, Any


class NightActivityDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            data['ts'] = pd.to_datetime(data['ts'])
            data['hour'] = data['ts'].dt.hour
            data['date'] = data['ts'].dt.date

            if 'ua_is_bot' in data.columns:
                data['is_bot'] = data['ua_is_bot'].fillna(0).astype(bool)
            else:
                data['is_bot'] = False

            # Ночная активность
            night_data = data[data['hour'].between(0, 7)]

            if night_data.empty:
                self.results = pd.DataFrame()
                return night_data

            # Аномалии: IP с >100 запросами
            anomalies = night_data.groupby(['ip', 'is_bot']).size().reset_index(name='requests')
            anomalies = anomalies[anomalies['requests'] > 100]
            self.anomalies = anomalies
            self.results = night_data

            return night_data

        except Exception as e:
            self.logger.error(f"Detection failed: {str(e)}")
            raise

    def generate_report(self) -> Dict[str, Any]:
        if self.results is None or self.results.empty:
            return {
                "summary": "No night activity detected",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        total_requests = len(self.results)
        total_bots = self.results['is_bot'].sum()
        bot_share = total_bots / total_requests if total_requests else 0

        hourly = self.results.groupby('hour').agg(
            total_requests=('ip', 'size'),
            unique_ips=('ip', 'nunique'),
            bots=('is_bot', 'sum')
        )

        anomalies_summary = {}
        if hasattr(self, 'anomalies') and not self.anomalies.empty:
            anomalies_summary = {
                "total_anomalous_ips": int(self.anomalies['ip'].nunique()),
                "bot_ips": int(self.anomalies[self.anomalies['is_bot']]['ip'].nunique()),
                "human_ips": int(self.anomalies[~self.anomalies['is_bot']]['ip'].nunique()),
                "max_requests": int(self.anomalies['requests'].max())
            }

        return {
            "summary": f"Night activity (00:00–07:00) detected: {total_requests} events, {bot_share:.1%} bots",
            "metrics": {
                "total_requests": total_requests,
                "unique_ips": int(self.results['ip'].nunique()),
                "bot_share": f"{bot_share:.1%}",
                **anomalies_summary
            },
            "tables": {
                "hourly_summary": hourly.reset_index(),
                "anomalous_ips": self.anomalies if hasattr(self, 'anomalies') else pd.DataFrame()
            },
            "plots": {
                "requests_by_hour": self._plot_hourly,
                "top_ips": self._plot_top_ips
            },
            "raw_data": self.results
        }

    def _plot_hourly(self):
        hourly = self.results.groupby('hour').size()
        plt.figure(figsize=(10, 5))
        hourly.plot(kind='bar', color='midnightblue')
        plt.title("Requests per Hour (Night)")
        plt.xlabel("Hour")
        plt.ylabel("Requests")
        plt.grid(True)

    def _plot_top_ips(self):
        top_ips = self.results['ip'].value_counts().head(10)
        plt.figure(figsize=(10, 5))
        top_ips.plot(kind='barh', color='darkred')
        plt.title("Top 10 Active IPs at Night")
        plt.xlabel("Requests")
        plt.gca().invert_yaxis()
        plt.grid(True)
