# core/detectors/isolation_forest.py
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
        
        # Логирование параметров инициализации
        self.logger.debug(
            f"[DEBUG] Initialized IsolationForest with: "
            f"contamination={self.contamination}, "
            f"n_estimators={self.n_estimators}, "
            f"interval_minutes={self.interval_minutes}, "
            f"time_column='{self.time_column}'"
        )

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            data = data.copy()
            
            # Логирование входящих данных
            self.logger.debug(f"[DEBUG] Input data shape: {data.shape}")
            if not data.empty:
                self.logger.debug(f"[DEBUG] Time range: {data[self.time_column].min()} to {data[self.time_column].max()}")

            if self.time_column not in data.columns:
                raise ValueError(f"Missing time column: {self.time_column}")

            data[self.time_column] = pd.to_datetime(data[self.time_column], errors='coerce')
            data['time_interval'] = data[self.time_column].dt.floor(f"{self.interval_minutes}min")

            # Агрегация активности
            agg = data.groupby('time_interval').agg(
                requests=('ip', 'count'),
                unique_ips=('ip', 'nunique')
            ).reset_index()
            
            # Логирование агрегированных данных
            self.logger.debug(f"[DEBUG] Aggregated data shape: {agg.shape}")
            if not agg.empty:
                self.logger.debug(
                    f"[DEBUG] Aggregated stats - Requests: mean={agg['requests'].mean():.1f}, "
                    f"max={agg['requests'].max()}; Unique IPs: mean={agg['unique_ips'].mean():.1f}, "
                    f"max={agg['unique_ips'].max()}"
                )

            if agg.empty or len(agg) < 10:
                raise ValueError("Not enough data for anomaly detection after aggregation.")

            # Логирование перед обучением модели
            self.logger.debug(
                f"[DEBUG] Training IsolationForest on {len(agg)} intervals "
                f"with contamination={self.contamination}"
            )

            # Запуск Isolation Forest
            model = IsolationForest(
                contamination=self.contamination,
                n_estimators=self.n_estimators,
                random_state=42
            )
            model.fit(agg[['requests', 'unique_ips']])

            agg['anomaly'] = model.predict(agg[['requests', 'unique_ips']])
            agg['anomaly'] = agg['anomaly'].map({1: 0, -1: 1})  # 1 - нормальные, -1 - аномалии
            
            # Логирование результатов детекции
            anomaly_count = agg['anomaly'].sum()
            self.logger.info(
                f"[INFO] Detected {anomaly_count} anomalous intervals "
                f"({anomaly_count/len(agg):.1%} of total)"
            )
            if anomaly_count > 0:
                self.logger.debug(
                    f"[DEBUG] Anomaly details - Max requests: {agg.loc[agg['anomaly'] == 1, 'requests'].max()}, "
                    f"Max unique IPs: {agg.loc[agg['anomaly'] == 1, 'unique_ips'].max()}"
                )

            self.results = agg
            return data

        except Exception as e:
            self.logger.error(f"Detection failed: {str(e)}")
            raise

    def generate_report(self) -> dict:
        if self.results is None or self.results.empty:
            self.logger.warning("[WARNING] No results to generate report from")
            return {
                "summary": "No anomalies detected by IsolationForest.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        anomalies = self.results[self.results['anomaly'] == 1]
        normal = self.results[self.results['anomaly'] == 0]
        
        # Логирование перед генерацией отчета
        self.logger.debug(
            f"[DEBUG] Generating report - {len(anomalies)} anomalies found, "
            f"{len(normal)} normal intervals"
        )

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
            
            # Логирование факта создания графика
            self.logger.debug("[DEBUG] Anomaly detection plot created")

        report = {
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
        
        self.logger.debug("[DEBUG] Report generated successfully")
        return report