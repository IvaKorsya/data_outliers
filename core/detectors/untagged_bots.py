from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt

class UntaggedBotsDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.top_n = self.config.get('top_n', 10)
        self.results = {}

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        if 'ua_is_bot' not in data.columns or 'ip' not in data.columns:
            raise ValueError("Required columns 'ua_is_bot' and 'ip' are missing.")

        df = data.copy()
        df['ua_is_bot'] = df['ua_is_bot'].fillna(0).astype(int)

        # Считаем общее количество запросов по IP
        ip_stats = df.groupby('ip').agg(
            total_requests=('ts', 'count'),
            bot_requests=('ua_is_bot', 'sum')
        ).reset_index()

        # Вычисляем скрытых ботов: много запросов, но не помечены как боты
        top_suspicious = ip_stats[
            (ip_stats['total_requests'] >= 100) & (ip_stats['bot_requests'] == 0)
        ].nlargest(self.top_n, 'total_requests')

        total_requests = len(df)
        total_bots = df['ua_is_bot'].sum()
        hidden_bots = top_suspicious['total_requests'].sum()
        human_requests = total_requests - total_bots - hidden_bots

        self.results = {
            'total_requests': total_requests,
            'total_bots': int(total_bots),
            'hidden_bots': int(hidden_bots),
            'human_requests': int(human_requests),
            'top_suspicious_ips': top_suspicious
        }

        return df

    def generate_report(self) -> dict:
        if not self.results:
            return {
                "summary": "No untagged bot activity detected.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        return {
            "summary": f"Detected {self.results['hidden_bots']} suspicious bot-like requests.",
            "metrics": {
                "total_requests": self.results['total_requests'],
                "total_bots": self.results['total_bots'],
                "hidden_bots": self.results['hidden_bots'],
                "human_requests": self.results['human_requests']
            },
            "tables": {
                "suspicious_ips": self.results['top_suspicious_ips']
            },
            "plots": {
                "bot_distribution": self._plot_pie
            }
        }

    def _plot_pie(self):
        labels = ['Humans', 'Known Bots', 'Hidden Bots']
        sizes = [
            self.results['human_requests'],
            self.results['total_bots'],
            self.results['hidden_bots']
        ]
        colors = ['#66b3ff', '#ff6666', '#ffc107']

        plt.figure(figsize=(8, 6))
        plt.pie(sizes, labels=[f"{l} ({s})" for l, s in zip(labels, sizes)],
                autopct='%1.1f%%', startangle=140, colors=colors)
        plt.title("Request Distribution: Humans vs Bots")
        plt.axis('equal')
        plt.tight_layout()
