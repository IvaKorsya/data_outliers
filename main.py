import argparse
from pathlib import Path
from core.runner import AnalysisRunner
from core.config_manager import ConfigManager
from detectors import (  # noqa: F401
    ActivitySpikesDetector,
    IsolationForestDetector,
    NightActivityDetector,
    NodeIdChecker,
    PageViewAnomalyDetector,
    UntaggedBotsDetector
)

def setup_framework(config_path: str) -> AnalysisRunner:
    """Инициализация фреймворка с загрузкой конфигурации"""
    # Загрузка конфига
    config = ConfigManager.get_instance(config_path)
    
    # Инициализация runner с конфигом
    runner = AnalysisRunner(config)
    
    # Автоматическая регистрация всех детекторов
    detectors = {
        'activity_spikes': ActivitySpikesDetector,
        'isolation_forest': IsolationForestDetector,
        'night_activity': NightActivityDetector,
        'node_id': NodeIdChecker,
        'page_view': PageViewAnomalyDetector,
        'untagged_bots': UntaggedBotsDetector
    }
    
    for name, detector in detectors.items():
        runner.register_detector(name, detector)
    
    return runner

def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config',
        type=str,
        default='configs/local.yaml',
        help='Path to config file'
    )
    parser.add_argument(
        '--detectors',
        type=str,
        default='all',
        help='Comma-separated list of detectors or "all"'
    )
    parser.add_argument(
        '--data-path',
        type=str,
        help='Override data path from config'
    )
    return parser.parse_args()

def main():
    args = parse_args()
    
    try:
        # Проверка существования конфига
        if not Path(args.config).exists():
            raise FileNotFoundError(f"Config file not found: {args.config}")
        
        # Инициализация
        runner = setup_framework(args.config)
        
        # Определение детекторов
        detectors = (
            list(runner.detectors.keys()) 
            if args.detectors == 'all' 
            else args.detectors.split(',')
        )
        
        # Переопределение пути к данным если нужно
        data_path = args.data_path or runner.config.data_loader.datasets_path
        
        # Запуск анализа
        runner.run(
            data_path=data_path,
            detectors=detectors,
            output_format=runner.config.reporting.default_format
        )
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
