from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, Any
import logging

class NodeIdCheckDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.required_columns = self.config.get(
            'required_columns', 
            ['url', 'node_id', 'content_type']
        )
        self.validate_hourly = self.config.get('validate_hourly', True)  # По умолчанию включим
        
    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        """Проверка корректности node_id"""
        try:
            # Проверка наличия обязательных колонок
            missing_cols = [col for col in self.required_columns if col not in data.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            results = {
                'invalid_node_ids': [],
                'hourly_issues': {},
                'checked_urls': data['url'].nunique(),
                'data_sample': data.head(1000)  # Сохраняем сэмпл для графиков
            }
            
            # Проверка уникальности node_id
            grouped = data.groupby(['url', 'node_id']).size().reset_index(name='count')
            duplicates = grouped[grouped.duplicated(['url'], keep=False)]
            
            if not duplicates.empty:
                results['invalid_node_ids'] = duplicates.to_dict('records')
            
            # Почасовой анализ
            if self.validate_hourly:
                data['hour'] = pd.to_datetime(data['ts']).dt.hour
                for hour, group in data.groupby('hour'):
                    hour_group = group.groupby(['url', 'node_id']).size().reset_index(name='count')
                    hour_duplicates = hour_group[hour_group.duplicated(['url'], keep=False)]
                    if not hour_duplicates.empty:
                        results['hourly_issues'][hour] = hour_duplicates.to_dict('records')
            
            self.results = results
            return data
            
        except Exception as e:
            self.logger.error(f"Detection failed: {str(e)}")
            raise

    def generate_report(self) -> Dict[str, Any]:
        if not hasattr(self, 'results'):
            raise ValueError("No results available. Run detect() first.")
            
        invalid_nodes = self.results['invalid_node_ids']
        hourly_issues = self.results['hourly_issues']
        
        # Формируем отчет
        report = {
            "summary": self._generate_summary(invalid_nodes, hourly_issues),
            "metrics": self._generate_metrics(invalid_nodes, hourly_issues),
            "tables": self._generate_tables(invalid_nodes),
            "plots": self._generate_plots(hourly_issues)
        }
        
        return report

    def _generate_summary(self, invalid_nodes, hourly_issues):
        lines = [
            "Node ID Validation Report",
            "="*40,
            f"Total URLs checked: {self.results['checked_urls']}",
            f"Conflicts found: {len(invalid_nodes)}",
            f"Hours with issues: {len(hourly_issues)}"
        ]
        if invalid_nodes:
            lines.extend([
                "\nSample conflicts:",
                *[f"- {x['url']} (Node IDs: {x['count']})" for x in invalid_nodes[:3]]
            ])
        return "\n".join(lines)

    def _generate_metrics(self, invalid_nodes, hourly_issues):
        return {
            "total_urls": self.results['checked_urls'],
            "conflict_urls": len(invalid_nodes),
            "conflict_rate": f"{len(invalid_nodes)/max(1, self.results['checked_urls']):.1%}",
            "peak_conflict_hour": max(hourly_issues.items(), key=lambda x: len(x[1]))[0] if hourly_issues else None
        }

    def _generate_tables(self, invalid_nodes):
        return {
            "top_conflicts": pd.DataFrame(invalid_nodes).sort_values('count', ascending=False).head(10)
        } if invalid_nodes else {}

    def _generate_plots(self, hourly_issues):
        if not hourly_issues:
            return {}
            
        def plot_conflicts_by_hour():
            plt.figure(figsize=(12, 6))
            hours, counts = zip(*sorted(
                [(h, len(issues)) for h, issues in hourly_issues.items()]
            ))
            plt.bar(hours, counts, color='#ff7f0e')
            plt.title('Node ID Conflicts by Hour')
            plt.xlabel('Hour of Day')
            plt.ylabel('Number of Conflicts')
            plt.grid(True, alpha=0.3)
            
        return {"hourly_conflicts": plot_conflicts_by_hour}