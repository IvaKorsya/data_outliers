from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt

class PageViewOrderDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.user_col = config.get("user_id_column", "randPAS_user_agent_id")
        self.session_col = config.get("session_id_column", "randPAS_session_id")
        self.results = pd.DataFrame()
        self.total_records = 0

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            data = data.copy()

            # Проверяем наличие нужных колонок
            for col in [self.user_col, self.session_col, 'page_view_order_number', 'event']:
                if col not in data.columns:
                    raise ValueError(f"Missing required column: {col}")

            # Фильтрация только page_view событий
            data = data[data['event'] == 'page_view']

            # Убираем пустые записи
            data = data.dropna(subset=[self.user_col, self.session_col, 'page_view_order_number'])

            # Приводим типы
            data[self.user_col] = data[self.user_col].astype(str)
            data[self.session_col] = data[self.session_col].astype(str)
            data['page_view_order_number'] = pd.to_numeric(data['page_view_order_number'], errors='coerce').fillna(0).astype(int)

            self.total_records = len(data)

            anomalies = []
            grouped = data.sort_values(['ts']).groupby([self.user_col, self.session_col])

            for (user_id, session_id), group in grouped:
                page_numbers = group['page_view_order_number'].tolist()
                for i in range(1, len(page_numbers)):
                    prev = page_numbers[i - 1]
                    curr = page_numbers[i]
                    delta = curr - prev

                    if curr < prev:
                        anomalies.append({
                            'user_id': user_id,
                            'session_id': session_id,
                            'event_index': i,
                            'previous': prev,
                            'current': curr,
                            'delta': delta,
                            'type': 'reset'
                        })
                    elif delta > 1:
                        anomalies.append({
                            'user_id': user_id,
                            'session_id': session_id,
                            'event_index': i,
                            'previous': prev,
                            'current': curr,
                            'delta': delta,
                            'type': 'skip'
                        })

            self.results = pd.DataFrame(anomalies)
            return data

        except Exception as e:
            self.logger.error(f"Detection failed: {str(e)}")
            raise

    def generate_report(self) -> dict:
        if self.results.empty:
            return {
                "summary": "No page view order anomalies detected.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        counts = self.results["type"].value_counts().to_dict()

        def plot():
            fig, axes = plt.subplots(1, 2, figsize=(12, 6))

            # 1. Pie chart по типам аномалий
            axes[0].pie(
                counts.values(),
                labels=[f"{k} ({v})" for k, v in counts.items()],
                autopct="%1.1f%%",
                startangle=90,
                colors=["#4ECDC4", "#FFD166"]
            )
            axes[0].set_title("Anomaly Type Distribution")

            # 2. Pie chart для общего соотношения нормальных и аномальных событий
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
            "summary": f"Found {len(self.results)} anomalies across sessions.",
            "metrics": {
                "total_anomalies": int(len(self.results)),
                **{k: int(v) for k, v in counts.items()}
            },
            "tables": {
                "anomalies": self.results
            },
            "plots": {
                "anomalies_summary": plot
            }
        }