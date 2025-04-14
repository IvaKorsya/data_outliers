import argparse
from pathlib import Path
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import json

from core.runner import AnalysisRunner
from core.config_manager import ConfigManager
from core.data_loader import DataLoader
from core.report_generator import ReportGenerator
from detectors import (
    ActivitySpikesDetector,
    IsolationForestDetector,
    NightActivityDetector,
    NodeIdChecker,
    PageViewAnomalyDetector,
    UntaggedBotsDetector,
    TestDetector 
)

def setup_framework(config_path: str) -> AnalysisRunner:
    """Инициализация фреймворка с загрузкой конфигурации"""
    config = ConfigManager(config_path).config
    runner = AnalysisRunner(config)
    
    # Автоматическая регистрация всех детекторов
    detectors = {
        'activity_spikes': ActivitySpikesDetector,
        'isolation_forest': IsolationForestDetector,
        'night_activity': NightActivityDetector,
        'node_id': NodeIdChecker,
        'page_view': PageViewAnomalyDetector,
        'untagged_bots': UntaggedBotsDetector
        'test_detector': TestDetector
    }
    
    for name, detector in detectors.items():
        runner.register_detector(name, detector)
    
    return runner

def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Anomaly Detection Framework')
    parser.add_argument(
        '--config',
        type=str,
        default='configs/default.yaml',
        help='Path to config file'
    )
    parser.add_argument(
        '--data-path',
        type=str,
        required=True,
        help='Path to data file or directory'
    )
    parser.add_argument(
        '--detectors',
        type=str,
        default='all',
        help='Comma-separated list of detectors or "all"'
    )
    parser.add_argument(
        '--output-format',
        type=str,
        default='console',
        choices=['console', 'json', 'html'],
        help='Output report format'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='reports',
        help='Directory to save reports'
    )
    return parser.parse_args()

def main():
    args = parse_args()
    
    try:
        # Инициализация фреймворка
        runner = setup_framework(args.config)
        
        # Определение детекторов для запуска
        detectors_to_run = (
            list(runner.detectors.keys()) 
            if args.detectors == 'all' 
            else [d.strip() for d in args.detectors.split(',')]
        )
        
        # Запуск анализа
        results = runner.run(
            data_path=args.data_path,
            detectors=detectors_to_run,
            output_format=args.output_format,
            output_dir=args.output_dir
        )
        
        # Сохранение сводного отчета
        if args.output_format != 'console':
            ReportGenerator().save_summary(
                results, 
                args.output_dir, 
                args.output_format
            )
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
