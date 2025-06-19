# core/detectors/night_activity.py
from core.base_detector import BaseAnomalyDetector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, Any


class NightActivityDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.logger.debug("[DEBUG] NightActivityDetector initialized with default configuration")

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            # Логирование входных данных
            self.logger.debug(f"[DEBUG] Starting night activity detection. Input rows: {len(data)}")
            if not data.empty:
                self.logger.debug(f"[DEBUG] Data time range: {data['ts'].min()} to {data['ts'].max()}")

            data['ts'] = pd.to_datetime(data['ts'])
            data['hour'] = data['ts'].dt.hour
            data['date'] = data['ts'].dt.date

            # Логирование информации о ботах
            if 'ua_is_bot' in data.columns:
                bot_count = data['ua_is_bot'].fillna(0).astype(bool).sum()
                self.logger.debug(f"[DEBUG] Found {bot_count} bot events in input data")
                data['is_bot'] = data['ua_is_bot'].fillna(0).astype(bool)
            else:
                self.logger.debug("[DEBUG] No bot detection data found")
                data['is_bot'] = False

            # Ночная активность
            night_data = data[data['hour'].between(0, 7)]
            self.logger.debug(f"[DEBUG] Found {len(night_data)} night events (00:00-07:00)")

            if night_data.empty:
                self.logger.warning("[WARNING] No night activity detected")
                self.results = pd.DataFrame()
                return night_data

            # Аномалии: IP с >100 запросами
            ip_activity = night_data.groupby(['ip', 'is_bot']).size().reset_index(name='requests')
            self.logger.debug(f"[DEBUG] Unique IPs at night: {len(ip_activity)}")
            
            anomalies = ip_activity[ip_activity['requests'] > 100]
            anomaly_count = len(anomalies)
            
            if anomaly_count > 0:
                self.logger.info(
                    f"[INFO] Found {anomaly_count} anomalous IPs with >100 night requests. "
                    f"Bots: {anomalies['is_bot'].sum()}, Humans: {anomaly_count - anomalies['is_bot'].sum()}"
                )
                self.logger.debug(
                    f"[DEBUG] Anomaly details - Max requests: {anomalies['requests'].max()}, "
                    f"Top IP: {anomalies.nlargest(1, 'requests').iloc[0]['ip']}"
                )
            else:
                self.logger.debug("[DEBUG] No IPs exceeded 100 night requests")

            self.anomalies = anomalies
            self.results = night_data

            return night_data

        except Exception as e:
            self.logger.error(f"[ERROR] Detection failed: {str(e)}")
            raise

    def generate_report(self) -> Dict[str, Any]:
        if self.results is None or self.results.empty:
            self.logger.warning("[WARNING] No results available for report generation")
            return {
                "summary": "No night activity detected",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        # Логирование статистики для отчета
        total_requests = len(self.results)
        total_bots = self.results['is_bot'].sum()
        bot_share = total_bots / total_requests if total_requests else 0
        
        self.logger.debug(
            f"[DEBUG] Preparing report - Total night requests: {total_requests}, "
            f"Bots: {total_bots} ({bot_share:.1%})"
        )

        hourly = self.results.groupby('hour').agg(
            total_requests=('ip', 'size'),
            unique_ips=('ip', 'nunique'),
            bots=('is_bot', 'sum')
        )
        self.logger.debug(f"[DEBUG] Hourly stats:\n{hourly}")

        anomalies_summary = {}
        if hasattr(self, 'anomalies') and not self.anomalies.empty:
            anomalies_summary = {
                "total_anomalous_ips": int(self.anomalies['ip'].nunique()),
                "bot_ips": int(self.anomalies[self.anomalies['is_bot']]['ip'].nunique()),
                "human_ips": int(self.anomalies[~self.anomalies['is_bot']]['ip'].nunique()),
                "max_requests": int(self.anomalies['requests'].max())
            }
            self.logger.debug(f"[DEBUG] Anomalies summary: {anomalies_summary}")

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
        self.logger.debug("[DEBUG] Generated hourly activity plot")

    def _plot_top_ips(self):
        top_ips = self.results['ip'].value_counts().head(10)
        plt.figure(figsize=(10, 5))
        top_ips.plot(kind='barh', color='darkred')
        plt.title("Top 10 Active IPs at Night")
        plt.xlabel("Requests")
        plt.gca().invert_yaxis()
        plt.grid(True)
        self.logger.debug("[DEBUG] Generated top IPs plot")