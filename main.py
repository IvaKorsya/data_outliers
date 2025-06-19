# main.py
import argparse
from pathlib import Path
from typing import Dict
import importlib
import logging
import sys
import warnings
import pandas as pd

from core.config_manager import ConfigManager
from core.runner import AnalysisRunner
from core.report_generator import ReportGenerator

# Встроенные детекторы
from core.detectors.activity_spikes import ActivitySpikesDetector
from core.detectors.node_id_check import NodeIdCheckDetector
from core.detectors.page_view import PageViewOrderDetector
from core.detectors.untagged_bots import UntaggedBotsDetector
from core.detectors.isolation_forest import IsolationForestDetector
from core.detectors.night_activity import NightActivityDetector
from core.detectors.users_devices import UsersDevicesDetector

# Подавление лишних предупреждений
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)
logging.getLogger('matplotlib.font_manager').setLevel(logging.WARNING)

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_detectors(detectors_dir: Path) -> Dict[str, type]:
    """Динамически загружает все детекторы из указанной директории"""
    detectors = {}
    for detector_file in detectors_dir.glob('*.py'):
        if detector_file.name == '__init__.py':
            continue
        try:
            module_name = f"core.detectors.{detector_file.stem}"
            module = importlib.import_module(module_name)
            for name, obj in module.__dict__.items():
                if (isinstance(obj, type) and 
                    hasattr(obj, 'detect') and 
                    hasattr(obj, 'generate_report')):
                    if detector_file.stem not in detectors:
                        detectors[detector_file.stem] = obj
                        logger.debug(f"Auto-loaded detector: {detector_file.stem}")
        except Exception as e:
            logger.warning(f"Failed to load {detector_file.name}: {e}")
    return detectors


def setup_framework(config_path: str) -> AnalysisRunner:
    config = ConfigManager(config_path).config
    runner = AnalysisRunner(config)

    # Ручная регистрация детекторов
    manual_detectors = {
        'activity_spikes': ActivitySpikesDetector,
        'node_id_check': NodeIdCheckDetector,
        'page_view': PageViewOrderDetector,
        'untagged_bots': UntaggedBotsDetector,
        'isolation_forest': IsolationForestDetector,
        'night_activity': NightActivityDetector,
        'users_devices': UsersDevicesDetector
    }

    for name, cls in manual_detectors.items():
        runner.register_detector(name, cls)

    # Автоматическая регистрация остальных детекторов
    all_auto = load_detectors(Path("core/detectors"))
    for name, cls in all_auto.items():
        if name not in runner.detectors:
            runner.register_detector(name, cls)

    return runner


def parse_args():
    parser = argparse.ArgumentParser(
        description='Anomaly Detection Framework',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--config', type=str, default='core/config/local.yaml', help='Path to config file')
    parser.add_argument('--data-path', type=str, required=True, help='Path to data file or directory')
    parser.add_argument('--detectors', type=str, default='all', help='Comma-separated list or "all"')
    parser.add_argument('--output-format', type=str, default='console', choices=['console', 'json', 'html'], help='Report format')
    parser.add_argument('--output-dir', type=str, default='outputs/reports', help='Directory to save reports')
    parser.add_argument('--log-level', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Logging level')

    return parser.parse_args()


def main():
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    try:
        logger.info("Initializing framework...")
        runner = setup_framework(args.config)

        if args.detectors.lower() == 'all':
            detectors_to_run = list(runner.detectors.keys())
            logger.info(f"Running ALL detectors: {', '.join(detectors_to_run)}")
        else:
            detectors_to_run = [d.strip() for d in args.detectors.split(',')]
            invalid = [d for d in detectors_to_run if d not in runner.detectors]
            if invalid:
                raise ValueError(f"Invalid detectors: {', '.join(invalid)}")
            logger.info(f"Running detectors: {', '.join(detectors_to_run)}")

        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        results = runner.run(
            data_path=args.data_path,
            detectors=detectors_to_run,
            output_format=args.output_format,
            output_dir=output_dir
        )

        if args.output_format != 'console':
            ReportGenerator().save_summary(results, output_dir, args.output_format)
            logger.info(f"Report saved to {output_dir}")

        logger.info("Analysis completed successfully")

    except Exception as e:
        logger.error(f"Framework error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()