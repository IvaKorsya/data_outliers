from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt

class NodeIdCheckDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.required_columns = ['node_id', 'main_rubric_id', 'content_is_longread', 'title']
        self.conflicts = pd.DataFrame()
        self.hourly_conflicts = pd.Series(dtype=int)

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        """Основная логика детектора отсутствующих node_id при наличии признаков контента."""
        for col in self.required_columns:
            if col not in data.columns:
                raise ValueError(f"Missing required column: {col}")

        data = data.copy()
        data['ts'] = pd.to_datetime(data['ts'], errors='coerce')
        data['hour'] = data['ts'].dt.hour

        # Условие: контент есть, а node_id пустой или 0
        mask_missing_node = (
            (data['node_id'].isna() | (data['node_id'] == 0)) &
            (
                data['main_rubric_id'].notna() |
                data['content_is_longread'].notna() |
                data['title'].notna()
            )
        )

        self.conflicts = data[mask_missing_node]

        if not self.conflicts.empty:
            self.hourly_conflicts = self.conflicts.groupby('hour').size()

        return data

    def generate_report(self) -> dict:
        """Формирование итогового отчета"""
        return {
            'summary': f"Detected {len(self.conflicts)} entries with missing node_id and content presence.",
            'metrics': {
                'conflict_count': int(len(self.conflicts)),
                'hours_with_issues': int(self.hourly_conflicts.count())
            },
            'tables': {
                'conflicts_sample': self.conflicts.head(100) if not self.conflicts.empty else pd.DataFrame()
            },
            'plots': {
                'hourly_conflicts_distribution': self._plot_hourly_conflicts
            }
        }

    def _plot_hourly_conflicts(self):
        """Построение графика распределения конфликтов по часам"""
        if self.hourly_conflicts.empty:
            return
        
        plt.figure(figsize=(12, 6))
        self.hourly_conflicts.sort_index().plot(kind='bar', color='tomato', edgecolor='black')
        plt.title('Distribution of Missing node_id Conflicts by Hour')
        plt.xlabel('Hour of the Day')
        plt.ylabel('Number of Conflicts')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=0)
        plt.tight_layout()