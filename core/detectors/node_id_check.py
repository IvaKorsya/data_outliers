from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt

class NodeIdCheckDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.required_columns = ['node_id', 'main_rubric_id', 'content_is_longread', 'title']
        self.conflicts = pd.DataFrame()
        self.hourly_conflicts = pd.Series(dtype=int)
        
        # Логирование инициализации
        self.logger.debug("[DEBUG] NodeIdCheckDetector initialized")
        self.logger.debug(f"[DEBUG] Required columns: {self.required_columns}")

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        """Основная логика детектора отсутствующих node_id при наличии признаков контента."""
        # Логирование входных данных
        self.logger.debug(f"[DEBUG] Starting detection. Input data shape: {data.shape}")
        if not data.empty:
            self.logger.debug(f"[DEBUG] Data time range: {data['ts'].min()} to {data['ts'].max()}")

        for col in self.required_columns:
            if col not in data.columns:
                error_msg = f"Missing required column: {col}"
                self.logger.error(f"[ERROR] {error_msg}")
                raise ValueError(error_msg)

        data = data.copy()
        data['ts'] = pd.to_datetime(data['ts'], errors='coerce')
        data['hour'] = data['ts'].dt.hour

        # Логирование статистики перед анализом
        self.logger.debug(
            f"[DEBUG] Data stats before detection:\n"
            f"Empty node_id: {data['node_id'].isna().sum()}\n"
            f"Zero node_id: {(data['node_id'] == 0).sum()}\n"
            f"Non-empty main_rubric_id: {data['main_rubric_id'].notna().sum()}"
        )

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
        
        # Логирование результатов детекции
        conflict_count = len(self.conflicts)
        if conflict_count > 0:
            self.logger.info(f"[INFO] Detected {conflict_count} entries with missing node_id and content presence")
            self.logger.debug(
                f"[DEBUG] Conflict details:\n"
                f"By main_rubric_id: {self.conflicts['main_rubric_id'].notna().sum()}\n"
                f"By longread: {self.conflicts['content_is_longread'].notna().sum()}\n"
                f"By title: {self.conflicts['title'].notna().sum()}"
            )
        else:
            self.logger.debug("[DEBUG] No conflicts detected")

        if not self.conflicts.empty:
            self.hourly_conflicts = self.conflicts.groupby('hour').size()
            self.logger.debug(f"[DEBUG] Hourly conflicts distribution:\n{self.hourly_conflicts.describe()}")

        return data

    def generate_report(self) -> dict:
        """Формирование итогового отчета"""
        if self.conflicts.empty:
            self.logger.debug("[DEBUG] No conflicts found for report generation")
            return {
                'summary': "Detected 0 entries with missing node_id and content presence.",
                'metrics': {
                    'conflict_count': 0,
                    'hours_with_issues': 0
                },
                'tables': {
                    'conflicts_sample': pd.DataFrame()
                },
                'plots': {
                    'hourly_conflicts_distribution': self._plot_hourly_conflicts
                }
            }

        # Логирование перед генерацией отчета
        self.logger.debug(
            f"[DEBUG] Generating report for {len(self.conflicts)} conflicts "
            f"across {self.hourly_conflicts.count()} hours"
        )

        report = {
            'summary': f"Detected {len(self.conflicts)} entries with missing node_id and content presence.",
            'metrics': {
                'conflict_count': int(len(self.conflicts)),
                'hours_with_issues': int(self.hourly_conflicts.count())
            },
            'tables': {
                'conflicts_sample': self.conflicts.head(100)
            },
            'plots': {
                'hourly_conflicts_distribution': self._plot_hourly_conflicts
            }
        }
        
        self.logger.debug("[DEBUG] Report generated successfully")
        return report

    def _plot_hourly_conflicts(self):
        """Построение графика распределения конфликтов по часам"""
        if self.hourly_conflicts.empty:
            self.logger.debug("[DEBUG] No data to plot hourly conflicts")
            return
        
        plt.figure(figsize=(12, 6))
        self.hourly_conflicts.sort_index().plot(kind='bar', color='tomato', edgecolor='black')
        plt.title('Distribution of Missing node_id Conflicts by Hour')
        plt.xlabel('Hour of the Day')
        plt.ylabel('Number of Conflicts')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=0)
        plt.tight_layout()
        self.logger.debug("[DEBUG] Hourly conflicts plot created")