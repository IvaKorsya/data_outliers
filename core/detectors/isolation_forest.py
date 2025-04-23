from core.base_detector import BaseAnomalyDetector
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt


class IsolationForestDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.contamination = self.config.get("contamination", 0.05)
        self.n_estimators = self.config.get("n_estimators", 100)
        self.interval_minutes = self.config.get("interval_minutes", 5)
        self.features = self.config.get("features", ["requests", "unique_ips"])

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            df = data.copy()
            df["ts"] = pd.to_datetime(df["ts"])
            df["is_bot"] = df.get("ua_is_bot", 0).fillna(0).astype(int) > 0

            # Агрегация по интервалу
            interval_str = f"{self.interval_minutes}min"
            df["time_interval"] = df["ts"].dt.floor(interval_str)
            grouped = df.groupby("time_interval").agg(
                requests=("ip", "count"),
                unique_ips=("ip", "nunique"),
                bot_ratio=("is_bot", "mean"),
                bot_count=("is_bot", "sum"),
                human_count=("is_bot", lambda x: len(x) - sum(x))
            ).reset_index()

            # Isolation Forest
            model = IsolationForest(
                contamination=self.contamination,
                n_estimators=self.n_estimators,
                random_state=42
            )
            preds = model.fit_predict(grouped[self.features])
            grouped["is_anomaly"] = preds == -1

            self.results = grouped
            self.anomalies = grouped[grouped["is_anomaly"]]
            return grouped

        except Exception as e:
            self.logger.error(f"Detection failed: {str(e)}")
            raise

    def generate_report(self) -> dict:
        if self.results is None or self.results.empty:
            return {
                "summary": "No data available.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        anomalies = self.anomalies
        total_anomalies = len(anomalies)

        return {
            "summary": f"Detected {total_anomalies} anomalous intervals using Isolation Forest",
            "metrics": {
                "total_anomalies": total_anomalies,
                "max_requests": int(anomalies["requests"].max()),
                "avg_requests": float(anomalies["requests"].mean()),
                "avg_bot_ratio": float(anomalies["bot_ratio"].mean()),
            },
            "tables": {
                "anomalies": anomalies.sort_values("requests", ascending=False).head(10)
            },
            "plots": {
                "anomalies_by_hour": self._plot_anomaly_distribution,
                "bot_vs_human_ratio": self._plot_pie_ratio,
                "scatter_requests_ips": self._plot_scatter,
                "requests_time_series": self._plot_time_series
            }
        }

    def _plot_anomaly_distribution(self):
        hourly = self.anomalies["time_interval"].dt.hour.value_counts().sort_index()
        plt.figure(figsize=(8, 4))
        hourly.plot(kind="bar", color="orange")
        plt.title("Аномалии по часам")
        plt.xlabel("Час")
        plt.ylabel("Число аномалий")
        plt.grid(axis='y', alpha=0.3)

    def _plot_pie_ratio(self):
        bot = self.anomalies["bot_count"].sum()
        human = self.anomalies["human_count"].sum()
        plt.figure(figsize=(5, 5))
        plt.pie([bot, human], labels=["Bots", "Humans"], colors=["red", "green"], autopct="%1.1f%%")
        plt.title("Боты vs Люди в аномалиях")

    def _plot_scatter(self):
        plt.figure(figsize=(6, 5))
        colors = np.where(self.results["is_anomaly"], "red", "blue")
        plt.scatter(
            self.results["requests"],
            self.results["unique_ips"],
            c=colors,
            alpha=0.6
        )
        plt.xlabel("Requests")
        plt.ylabel("Unique IPs")
        plt.title("Запросы vs Уникальные IP (красные — аномалии)")
        plt.grid(True, alpha=0.3)

    def _plot_time_series(self):
        plt.figure(figsize=(10, 5))
        plt.plot(self.results["time_interval"], self.results["requests"], label="Requests", color="blue")
        plt.scatter(
            self.anomalies["time_interval"],
            self.anomalies["requests"],
            color="red",
            label="Anomalies"
        )
        plt.title("Временной ряд с аномалиями")
        plt.xlabel("Время")
        plt.ylabel("Запросы")
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
