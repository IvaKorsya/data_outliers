from core.base_detector import BaseAnomalyDetector
import pandas as pd
from typing import Dict, Any
import logging

class NodeIdCheckDetector(BaseAnomalyDetector):
def init(self, config=None):
super().init(config)
self.required_columns = self.config.get(
'required_columns',
['url', 'node_id', 'content_type']
)
self.validate_hourly = self.config.get('validate_hourly', False)
def detect(self, data: pd.DataFrame) -> pd.DataFrame:
    """Проверка корректности node_id"""
    try:
        # Проверка наличия обязательных колонок
        missing_cols = [
            col for col in self.required_columns 
            if col not in data.columns
        ]
        if missing_cols:
            raise ValueError(
                f"Missing required columns: {missing_cols}"
            )
        
        results = {
            'invalid_node_ids': [],
            'hourly_issues': {}
        }
        
        # Проверка уникальности node_id для каждого url
        grouped = data.groupby(['url', 'node_id']).size().reset_index(name='count')
        duplicates = grouped[grouped.duplicated(['url'], keep=False)]
        
        if not duplicates.empty:
            results['invalid_node_ids'] = duplicates.to_dict('records')
        
        # Почасовая проверка (если включено)
        if self.validate_hourly:
            data['hour'] = pd.to_datetime(data['ts']).dt.hour
            hourly_issues = {}
            
            for hour, group in data.groupby('hour'):
                hour_duplicates = group.groupby(['url', 'node_id']).size().reset_index(name='count')
                hour_duplicates = hour_duplicates[hour_duplicates.duplicated(['url'], keep=False)]
                
                if not hour_duplicates.empty:
                    hourly_issues[hour] = hour_duplicates.to_dict('records')
            
            results['hourly_issues'] = hourly_issues
        
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
    
    def plot_issues():
        if not hourly_issues:
            return
            
        hours = list(hourly_issues.keys())
        counts = [len(v) for v in hourly_issues.values()]
        
        plt.bar(hours, counts)
        plt.title('Node ID Issues by Hour')
        plt.xlabel('Hour of Day')
        plt.ylabel('Number of Issues')
    
    summary_parts = []
    if invalid_nodes:
        summary_parts.append(
            f"Found {len(invalid_nodes)} URLs with multiple node IDs"
        )
    else:
        summary_parts.append("No node ID conflicts found")
        
    if hourly_issues:
        summary_parts.append(
            f"Hourly issues detected in {len(hourly_issues)} hours"
        )
    
    return {
        "summary": "\n".join(summary_parts),
        "metrics": {
            "total_conflicts": len(invalid_nodes),
            "hours_with_issues": len(hourly_issues),
            "max_issues_per_hour": max(
                [len(v) for v in hourly_issues.values()] or [0]
            )
        },
        "tables": {
            "node_id_conflicts": pd.DataFrame(invalid_nodes),
            "hourly_issues": pd.DataFrame({
                'hour': hourly_issues.keys(),
                'issue_count': [len(v) for v in hourly_issues.values()]
            })
        },
        "plots": {
            "hourly_issues": plot_issues
        } if hourly_issues else {}
    }
