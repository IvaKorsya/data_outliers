from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt

class UntaggedBotsDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.top_n = self.config.get('top_n', 10)
        self.results = {}
        
        # Логирование инициализации
        self.logger.debug("[DEBUG] UntaggedBotsDetector initialized")
        self.logger.debug(f"[DEBUG] Configuration - top_n: {self.top_n}")

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        # Проверка обязательных колонок
        if 'ua_is_bot' not in data.columns or 'ip' not in data.columns:
            error_msg = "Required columns 'ua_is_bot' and 'ip' are missing."
            self.logger.error(f"[ERROR] {error_msg}")
            raise ValueError(error_msg)

        df = data.copy()
        
        # Логирование входных данных
        self.logger.debug(f"[DEBUG] Starting detection. Input rows: {len(df)}")
        self.logger.debug(f"[DEBUG] Initial bot tags: {df['ua_is_bot'].sum()}")

        df['ua_is_bot'] = df['ua_is_bot'].fillna(0).astype(int)

        # Считаем общее количество запросов по IP
        ip_stats = df.groupby('ip').agg(
            total_requests=('ts', 'count'),
            bot_requests=('ua_is_bot', 'sum')
        ).reset_index()
        
        self.logger.debug(f"[DEBUG] Unique IPs analyzed: {len(ip_stats)}")
        self.logger.debug(f"[DEBUG] Top normal IP stats:\n{ip_stats.nlargest(3, 'total_requests')}")

        # Вычисляем скрытых ботов: много запросов, но не помечены как боты
        top_suspicious = ip_stats[
            (ip_stats['total_requests'] >= 100) & (ip_stats['bot_requests'] == 0)
        ].nlargest(self.top_n, 'total_requests')
        
        # Логирование подозрительных IP
        if not top_suspicious.empty:
            self.logger.info(
                f"[INFO] Found {len(top_suspicious)} suspicious IPs with "
                f"total {top_suspicious['total_requests'].sum()} requests"
            )
            self.logger.debug(
                f"[DEBUG] Top suspicious IPs:\n"
                f"Max requests: {top_suspicious['total_requests'].max()}\n"
                f"Min requests in top: {top_suspicious['total_requests'].min()}"
            )
        else:
            self.logger.debug("[DEBUG] No suspicious IPs detected")

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
        
        # Логирование итоговой статистики
        self.logger.debug(
            f"[DEBUG] Request distribution:\n"
            f"Total: {total_requests}\n"
            f"Known bots: {total_bots}\n"
            f"Hidden bots: {hidden_bots}\n"
            f"Human requests: {human_requests}"
        )

        return df

    def generate_report(self) -> dict:
        if not self.results:
            self.logger.warning("[WARNING] No results available for report generation")
            return {
                "summary": "No untagged bot activity detected.",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        # Логирование перед генерацией отчета
        self.logger.debug(
            f"[DEBUG] Generating report with:\n"
            f"Hidden bots: {self.results['hidden_bots']}\n"
            f"Top suspicious IPs: {len(self.results['top_suspicious_ips'])}"
        )

        report = {
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
        
        self.logger.debug("[DEBUG] Report generated successfully")
        return report

    def _plot_pie(self):
        if not self.results or self.results['total_requests'] == 0:
            self.logger.debug("[DEBUG] No data available for pie chart")
            return

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
        self.logger.debug("[DEBUG] Bot distribution pie chart created")