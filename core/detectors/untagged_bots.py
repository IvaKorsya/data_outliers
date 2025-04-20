from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class UntaggedBotsDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.top_n = self.config.get("top_n", 10)

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            data = data.copy()

            data["ts"] = pd.to_datetime(data["ts"])
            data["date"] = data["ts"].dt.date
            data["hour"] = data["ts"].dt.hour

            if "ua_is_bot" in data.columns:
                data["is_bot"] = pd.to_numeric(data["ua_is_bot"], errors="coerce").fillna(0) > 0
            else:
                data["is_bot"] = False

            ip_counts = data["ip"].value_counts().rename("request_count")
            data = data.merge(ip_counts.to_frame(), left_on="ip", right_index=True)

            # Подозрительные User-Agent
            if "ua_header" in data.columns:
                data["suspicious_ua"] = data["ua_header"].str.contains(
                    "bot|spider|crawl", case=False, na=False
                )
                data["is_googlebot_like"] = data["ua_header"].str.contains("googlebot", case=False, na=False)
            else:
                data["suspicious_ua"] = False
                data["is_googlebot_like"] = False

            # Логика скрытого бота
            data["is_hidden_bot"] = (data["is_bot"] == False) & (
                (data["request_count"] > 100) | data["suspicious_ua"]
            )

            # Финальное объединение
            data["is_bot"] = data["is_bot"] | data["is_hidden_bot"]

            self.results = data
            return data

        except Exception as e:
            self.logger.error(f"Detection failed: {str(e)}")
            raise

    def generate_report(self) -> dict:
        if self.results is None or self.results.empty:
            return {
                "summary": "No bots detected.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        df = self.results
        total = len(df)
        total_bots = df["is_bot"].sum()
        hidden_bots = df["is_hidden_bot"].sum()
        googlebots = df["is_googlebot_like"].sum()

        summary = (
            f"Detected {total_bots} bots ({total_bots/total:.1%}), "
            f"{hidden_bots} hidden ({hidden_bots/total:.1%}), "
            f"{googlebots} Googlebot-like ({googlebots/total:.1%})"
        )

        top_bots = df[df["is_bot"]].groupby("ip").agg(
            total_requests=("request_count", "max"),
            first_seen=("ts", "min"),
            last_seen=("ts", "max"),
            is_hidden=("is_hidden_bot", "any"),
            is_googlebot_like=("is_googlebot_like", "any")
        ).sort_values("total_requests", ascending=False).head(self.top_n).reset_index()

        return {
            "summary": summary,
            "metrics": {
                "total_requests": total,
                "total_bots": int(total_bots),
                "hidden_bots": int(hidden_bots),
                "googlebot_like_count": int(googlebots),
                "bot_ratio": f"{total_bots/total:.1%}",
                "hidden_bot_ratio": f"{hidden_bots/total:.1%}",
                "googlebot_like_ratio": f"{googlebots/total:.1%}"
            },
            "tables": {
                "top_bots": top_bots
            },
            "plots": {
                "bot_distribution": self._plot_distribution,
                "hourly_activity": self._plot_hourly
            }
        }

    def _plot_distribution(self):
        df = self.results
        humans = len(df) - df["is_bot"].sum()
        bots = df["is_bot"].sum() - df["is_hidden_bot"].sum()
        hidden = df["is_hidden_bot"].sum()

        plt.figure(figsize=(6, 6))
        plt.bar(["Humans", "Bots", "Hidden Bots"], [humans, bots, hidden],
                color=["green", "red", "orange"])
        plt.title("Request Distribution")
        plt.ylabel("Count")
        plt.grid(axis='y', alpha=0.3)

    def _plot_hourly(self):
        df = self.results
        hourly = df.groupby("hour").size()
        plt.figure(figsize=(10, 4))
        hourly.plot(kind="bar", color="blue", alpha=0.7)
        plt.title("Activity by Hour")
        plt.xlabel("Hour")
        plt.ylabel("Requests")
        plt.grid(True, alpha=0.3)
