# core/detectors/page_view.py
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
        
        # Логирование инициализации
        self.logger.debug("[DEBUG] PageViewOrderDetector initialized")
        self.logger.debug(
            f"[DEBUG] Configuration:\n"
            f"User column: {self.user_col}\n"
            f"Session column: {self.session_col}"
        )

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            data = data.copy()
            
            # Логирование входных данных
            self.logger.debug(f"[DEBUG] Starting detection. Input rows: {len(data)}")
            if not data.empty:
                self.logger.debug(f"[DEBUG] Event distribution:\n{data['event'].value_counts().to_dict()}")

            # Проверяем наличие нужных колонок
            required_cols = [self.user_col, self.session_col, 'page_view_order_number', 'event']
            for col in required_cols:
                if col not in data.columns:
                    error_msg = f"Missing required column: {col}"
                    self.logger.error(f"[ERROR] {error_msg}")
                    raise ValueError(error_msg)

            # Фильтрация только page_view событий
            data = data[data['event'] == 'page_view']
            self.logger.debug(f"[DEBUG] Page view events after filtering: {len(data)}")

            # Убираем пустые записи
            data = data.dropna(subset=[self.user_col, self.session_col, 'page_view_order_number'])
            self.logger.debug(f"[DEBUG] Valid records after NA removal: {len(data)}")

            # Приводим типы
            data[self.user_col] = data[self.user_col].astype(str)
            data[self.session_col] = data[self.session_col].astype(str)
            data['page_view_order_number'] = pd.to_numeric(data['page_view_order_number'], errors='coerce').fillna(0).astype(int)

            self.total_records = len(data)
            self.logger.debug(f"[DEBUG] Processing {self.total_records} valid page view records")

            anomalies = []
            grouped = data.sort_values(['ts']).groupby([self.user_col, self.session_col])
            session_count = grouped.ngroups
            self.logger.debug(f"[DEBUG] Analyzing {session_count} user sessions")

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
            
            # Логирование результатов
            if not self.results.empty:
                anomaly_types = self.results['type'].value_counts().to_dict()
                self.logger.info(
                    f"[INFO] Found {len(self.results)} anomalies in {session_count} sessions\n"
                    f"Reset anomalies: {anomaly_types.get('reset', 0)}\n"
                    f"Skip anomalies: {anomaly_types.get('skip', 0)}"
                )
                self.logger.debug(
                    f"[DEBUG] Anomaly details:\n"
                    f"Max reset delta: {self.results[self.results['type'] == 'reset']['delta'].min()}\n"
                    f"Max skip delta: {self.results[self.results['type'] == 'skip']['delta'].max()}"
                )
            else:
                self.logger.debug("[DEBUG] No page view order anomalies detected")

            return data

        except Exception as e:
            self.logger.error(f"[ERROR] Detection failed: {str(e)}")
            raise

    def generate_report(self) -> dict:
        if self.results.empty:
            self.logger.debug("[DEBUG] No anomalies found for report generation")
            return {
                "summary": "No page view order anomalies detected.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        counts = self.results["type"].value_counts().to_dict()
        
        # Логирование перед генерацией отчета
        self.logger.debug(
            f"[DEBUG] Generating report for {len(self.results)} anomalies\n"
            f"Total records processed: {self.total_records}\n"
            f"Anomaly rate: {len(self.results)/self.total_records:.2%}"
        )

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
            self.logger.debug("[DEBUG] Anomaly summary plots created")

        report = {
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
        
        self.logger.debug("[DEBUG] Report generated successfully")
        return report