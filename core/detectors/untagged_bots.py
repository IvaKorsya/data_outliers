# DETECTOR 1: UntaggedBotsDetector (адаптирован под реальные данные)
from core.base_detector import BaseAnomalyDetector
import pandas as pd

class UntaggedBotsDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.top_n = self.config.get('top_n', 10)

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        data['ua_is_bot'] = data['ua_is_bot'].fillna(0).astype(int)
        ip_counts = data['ip'].value_counts().rename('request_count')
        data = data.merge(ip_counts.to_frame(), left_on='ip', right_index=True)

        data['is_hidden_bot'] = (data['ua_is_bot'] == 0) & (data['request_count'] > 100)
        data['is_bot'] = data['ua_is_bot'].astype(bool) | data['is_hidden_bot']

        self.top_bots = (
            data[data['is_bot']].groupby('ip')
            .agg(total_requests=('request_count', 'max'))
            .sort_values('total_requests', ascending=False).head(self.top_n)
        )
        self.bot_summary = {
            'total_requests': len(data),
            'total_bots': data['is_bot'].sum(),
            'hidden_bots': data['is_hidden_bot'].sum()
        }
        return data

    def generate_report(self) -> dict:
        return {
            'summary': f"Detected {self.bot_summary['total_bots']} bots ({self.bot_summary['hidden_bots']} hidden)",
            'metrics': {
                'total_requests': self.bot_summary['total_requests'],
                'total_bots': self.bot_summary['total_bots'],
                'hidden_bots': self.bot_summary['hidden_bots'],
            },
            'tables': {
                'top_bots': self.top_bots.reset_index()
            },
            'plots': {}
        }