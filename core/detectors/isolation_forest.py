# core/detectors/activity_spikes.py
from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest

class IsolationForestDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.contamination = config.get('contamination', 0.05)
        self.n_estimators = config.get('n_estimators', 100)
        self.interval_minutes = config.get('interval_minutes', 5)
        self.time_column = config.get('time_column', 'ts')

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            data = data.copy()

            if self.time_column not in data.columns:
                raise ValueError(f"Missing time column: {self.time_column}")

            data[self.time_column] = pd.to_datetime(data[self.time_column], errors='coerce')
            data['time_interval'] = data[self.time_column].dt.floor(f"{self.interval_minutes}min")

            # Агрегация активности
            agg = data.groupby('time_interval').agg(
                requests=('ip', 'count'),
                unique_ips=('ip', 'nunique')
            ).reset_index()

            if agg.empty or len(agg) < 10:
                raise ValueError("Not enough data for anomaly detection after aggregation.")

            # Запуск Isolation Forest
            model = IsolationForest(
                contamination=self.contamination,
                n_estimators=self.n_estimators,
                random_state=42
            )
            model.fit(agg[['requests', 'unique_ips']])

            agg['anomaly'] = model.predict(agg[['requests', 'unique_ips']])
            agg['anomaly'] = agg['anomaly'].map({1: 0, -1: 1})  # 1 - нормальные, -1 - аномалии

            self.results = agg
            return data

        except Exception as e:
            self.logger.error(f"Detection failed: {str(e)}")
            raise

    def generate_report(self) -> dict:
        if self.results is None or self.results.empty:
            return {
                "summary": "No anomalies detected by IsolationForest.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        anomalies = self.results[self.results['anomaly'] == 1]
        normal = self.results[self.results['anomaly'] == 0]

        def plot():
            plt.figure(figsize=(14, 7))
            plt.scatter(
                normal['requests'], normal['unique_ips'],
                color='blue', label='Normal', alpha=0.5
            )
            plt.scatter(
                anomalies['requests'], anomalies['unique_ips'],
                color='red', label='Anomalies', marker='x'
            )
            plt.title("Isolation Forest Anomaly Detection")
            plt.xlabel("Requests per Interval")
            plt.ylabel("Unique IPs per Interval")
            plt.legend()
            plt.grid(True)
            plt.tight_layout()

        return {
            "summary": f"Detected {len(anomalies)} anomalies with Isolation Forest.",
            "metrics": {
                "total_intervals": int(len(self.results)),
                "total_anomalies": int(len(anomalies)),
                "max_requests_in_anomalies": int(anomalies['requests'].max()) if not anomalies.empty else 0,
                "max_unique_ips_in_anomalies": int(anomalies['unique_ips'].max()) if not anomalies.empty else 0
            },
            "tables": {
                "anomalous_intervals": anomalies
            },
            "plots": {
                "anomalies_plot": plot
            }
        }
