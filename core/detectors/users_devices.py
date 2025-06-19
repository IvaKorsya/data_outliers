from core.base_detector import BaseAnomalyDetector
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class UsersDevicesDetector(BaseAnomalyDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.required_columns = self.config.get(
            'required_columns',
            ['ts', 'ua_is_tablet', 'ua_is_mobile', 'ua_is_pc', 'randPAS_session_id']
        )
        self.logger.debug(f"Initialized with required columns: {self.required_columns}")

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        self.logger.debug("Starting detection with %d rows", len(data))
        df = self.load_data(data)
        df = self.identify_anomalies(df)
        self.results = self.analyze_device_usage(df)
        self.logger.debug("Detection completed successfully")
        return self.results

    def load_data(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            df = data.copy()
            df['ts'] = pd.to_datetime(df['ts'])
            df['date'] = df['ts'].dt.date
            df['hour'] = df['ts'].dt.hour
            
            for col in ['ua_is_tablet', 'ua_is_mobile', 'ua_is_pc']:
                if col in df.columns:
                    df[col] = df[col].fillna(False)
                    self.logger.debug("Found %d %s devices", df[col].sum(), col)
                else:
                    df[col] = False
                    self.logger.warning("Missing device column: %s", col)

            if 'randPASS_session_id' in df.columns:
                df = df.rename(columns={'randPASS_session_id': 'randPAS_session_id'})
                self.logger.debug("Renamed randPASS_session_id to randPAS_session_id")
            elif 'randPAS_session_id' not in df.columns:
                df['randPAS_session_id'] = None
                self.logger.warning("randPAS_session_id not found, using None")

            required_columns = ['randPAS_session_id', 'ua_is_tablet', 'ua_is_pc', 'ua_is_mobile']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                error_msg = f"Missing required columns: {', '.join(missing_columns)}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            return df
        
        except Exception as e:
            self.logger.error("Data loading failed: %s", str(e))
            raise

    def identify_anomalies(self, df):
        session_stats = df.groupby('randPAS_session_id')[['ua_is_tablet', 'ua_is_mobile', 'ua_is_pc']].agg(
            device_types=('ua_is_tablet', lambda x: (x.sum() > 0) + (df.loc[x.index, 'ua_is_mobile'].sum() > 0) + (df.loc[x.index, 'ua_is_pc'].sum() > 0)),
            has_null=('ua_is_tablet', lambda x: ((x == 0) & (df.loc[x.index, 'ua_is_mobile'] == 0) & (df.loc[x.index, 'ua_is_pc'] == 0)).any())
        )
        
        multi_device = session_stats[session_stats['device_types'] > 1]
        null_device = session_stats[session_stats['has_null']]
        
        df['is_multi_device_session'] = df['randPAS_session_id'].isin(multi_device.index)
        df['is_null_device_session'] = df['randPAS_session_id'].isin(null_device.index)
        
        self.logger.debug("Found %d multi-device and %d null-device sessions", 
                         len(multi_device), len(null_device))
        return df

    def analyze_device_usage(self, df):
        simultaneous_mask = (df[['ua_is_tablet', 'ua_is_pc', 'ua_is_mobile']].sum(axis=1) > 1)
        sim_count = simultaneous_mask.sum()
        
        if 'is_multi_device_session' in df.columns:
            multi_sessions = df['is_multi_device_session'].sum()
            null_sessions = df['is_null_device_session'].sum()
            single_sessions = len(df['randPAS_session_id'].unique()) - multi_sessions - null_sessions
        else:
            multi_sessions = null_sessions = single_sessions = 0
        
        device_stats = df[['ua_is_tablet', 'ua_is_mobile', 'ua_is_pc']].mean()
        hourly_activity = df.groupby(df['ts'].dt.hour)['randPAS_session_id'].nunique()
        
        self.logger.debug("Device proportions - PC: %.2f, Mobile: %.2f, Tablet: %.2f",
                         device_stats['ua_is_pc'], device_stats['ua_is_mobile'], device_stats['ua_is_tablet'])
        
        return {
            'device_proportions': device_stats,
            'hourly_activity': hourly_activity,
            'multi_device_sessions': multi_sessions,
            'null_device_sessions': null_sessions,
            'single_device_sessions': single_sessions,
            'simultaneous_devices': sim_count,
            'simultaneous_usage': df[simultaneous_mask]
        }

    def generate_report(self) -> dict:
        if not hasattr(self, 'results') or not self.results:
            self.logger.warning("No results available for reporting")
            return {
                "summary": "No device usage data available",
                "metrics": {},
                "tables": {},
                "plots": {}
            }

        metrics = {
            "total_sessions": (self.results['single_device_sessions'] + 
                             self.results['multi_device_sessions'] + 
                             self.results['null_device_sessions']),
            "pc_users": float(self.results['device_proportions']['ua_is_pc']),
            "mobile_users": float(self.results['device_proportions']['ua_is_mobile']),
            "tablet_users": float(self.results['device_proportions']['ua_is_tablet']),
            "multi_device_sessions": self.results['multi_device_sessions'],
            "null_device_sessions": self.results['null_device_sessions'],
            "simultaneous_usage": self.results['simultaneous_devices'],
            "peak_hour": int(self.results['hourly_activity'].idxmax()),
            "sessions_in_peak": int(self.results['hourly_activity'].max())
        }

        self.logger.info(
            "Report metrics - Total: %d, PC: %.1f%%, Mobile: %.1f%%, Multi-device: %d",
            metrics["total_sessions"],
            metrics["pc_users"]*100,
            metrics["mobile_users"]*100,
            metrics["multi_device_sessions"]
        )

        return {
            "summary": (
                f"Device usage analysis:\n"
                f"- Single device sessions: {metrics['total_sessions']}\n"
                f"- Multi-device sessions: {metrics['multi_device_sessions']}\n"
                f"- Null device sessions: {metrics['null_device_sessions']}\n"
                f"- Simultaneous device usage: {metrics['simultaneous_usage']} rows"
            ),
            "metrics": metrics,
            "tables": {
                "device_usage": pd.DataFrame({
                    'Device Type': ['PC', 'Mobile', 'Tablet'],
                    'Proportion': [
                        metrics['pc_users'],
                        metrics['mobile_users'],
                        metrics['tablet_users']
                    ]
                }).round(3),
                "hourly_activity": self.results['hourly_activity'].reset_index(
                    name='sessions').rename(columns={'ts': 'hour'}),
                "simultaneous_usage": self.results['simultaneous_usage']
            },
            "plots": {
                "device_usage_plot": self._plot_pie,
                "hourly_activity_plot": self._plot_hourly
            }
        }

    def _plot_pie(self):
        if not hasattr(self, 'results'):
            return

        plt.figure(figsize=(8, 6))
        props = self.results['device_proportions']
        labels = ['PC', 'Mobile', 'Tablet']
        sizes = [props['ua_is_pc'], props['ua_is_mobile'], props['ua_is_tablet']]
        
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140,
                colors=['skyblue', 'lightgreen', 'lightcoral'])
        plt.title('Device Usage Proportions', fontsize=14)
        plt.ylabel('')

    def _plot_hourly(self):
        if not hasattr(self, 'results'):
            return

        plt.figure(figsize=(10, 5))
        self.results['hourly_activity'].plot(kind='bar', color='purple')
        plt.title('User Activity by Hour', fontsize=14) 
        plt.xlabel('Hour of Day')
        plt.ylabel('Active Sessions')
        plt.xticks(rotation=0)
        plt.tight_layout()

