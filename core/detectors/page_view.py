from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt

class PageViewOrderDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.user_col = config.get("user_id_column", "randPAS_user_agent_id")
        self.session_col = config.get("session_id_column", "randPAS_session_id")

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            data = data.copy()
            data = data[data["ua_is_bot"] != 1]
            data = data.drop_duplicates()

            if "page_view_order_number" not in data.columns:
                raise ValueError("Missing 'page_view_order_number' column")

            # Удаление NaN перед типизацией
            if data["page_view_order_number"].isna().any():
                self.logger.warning("Found NaNs in 'page_view_order_number'. Dropping them.")
                data = data[data["page_view_order_number"].notna()]

            data["page_view_order_number"] = data["page_view_order_number"].astype(int)

            anomalies = []
            for (user_id, session_id), group in data.groupby([self.user_col, self.session_col]):
                page_numbers = group["page_view_order_number"].values
                for i in range(1, len(page_numbers)):
                    prev = page_numbers[i - 1]
                    curr = page_numbers[i]
                    delta = curr - prev

                    if curr < prev:
                        anomalies.append({
                            "user_id": user_id,
                            "session_id": session_id,
                            "event_index": i,
                            "current": curr,
                            "previous": prev,
                            "delta": delta,
                            "type": "reset"
                        })
                    elif delta > 1:
                        anomalies.append({
                            "user_id": user_id,
                            "session_id": session_id,
                            "event_index": i,
                            "current": curr,
                            "previous": prev,
                            "delta": delta,
                            "type": "skip"
                        })

            self.results = pd.DataFrame(anomalies)
            self.total_records = len(data)
            return data
        except Exception as e:
            self.logger.error(f"Detection failed: {str(e)}")
            raise

    def generate_report(self) -> dict:
        if self.results is None or self.results.empty:
            return {
                "summary": "No page view order anomalies detected.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        summary = f"Found {len(self.results)} anomalies across sessions."
        counts = self.results["type"].value_counts().to_dict()

        def plot():
            fig, axes = plt.subplots(1, 2, figsize=(12, 6))
            # Pie chart of anomaly types
            axes[0].pie(
                counts.values(),
                labels=[f"{k} ({v})" for k, v in counts.items()],
                autopct="%1.1f%%",
                startangle=90,
                colors=["#4ECDC4", "#FFD166"]
            )
            axes[0].set_title("Anomaly Type Distribution")

            sizes = [self.total_records - len(self.results), len(self.results)]
            labels = ["Normal", "Anomalous"]
            axes[1].pie(
                sizes,
                labels=[f"{l} ({s})" for l, s in zip(labels, sizes)],
                autopct="%1.1f%%",
                startangle=90,
                colors=["#66b3ff", "#ff9999"]
            )
            axes[1].set_title("Normal vs Anomalous Records")

            plt.tight_layout()

        return {
            "summary": summary,
            "metrics": {
                "total_anomalies": len(self.results),
                **counts
            },
            "tables": {
                "anomalies": self.results
            },
            "plots": {
                "anomalies_summary": plot
            }
        }
