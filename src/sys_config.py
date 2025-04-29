
import json
import os
import logging

CONFIG_FILE = "../config/es_config.json"
DEFAULT_CONFIG = {
    "opensearch": {
        "host": "https://localhost:9200",
        "user": "admin",
        "password": "123@QWE#asd",
        "index_name": "medical_records",
    },
    "app_settings": {
        "pdf_directory": "/Users/john/Data/projects/es_test/test/pdf_files/",
        "scan_interval_seconds": 300
    },
    "database": {
        "db_path": "indexed_files.db"
    }
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SysConfig:
    # --- 配置和状态管理 ---
    @staticmethod
    def load_config():
        """加载配置，如果文件不存在则使用默认配置并保存"""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # 确保所有默认键都存在，防止旧配置文件缺少新设置
                    for section, defaults in DEFAULT_CONFIG.items():
                        if section not in config:
                            config[section] = defaults
                        else:
                            for key, value in defaults.items():
                                if key not in config[section]:
                                    config[section][key] = value
                    return config
            except json.JSONDecodeError:
                logger.error(f"Error decoding JSON from {CONFIG_FILE}. Using default config.")
                return DEFAULT_CONFIG
        else:
            logger.info(f"{CONFIG_FILE} not found. Creating with default config.")
            SysConfig.save_config(DEFAULT_CONFIG)
            return DEFAULT_CONFIG

    @staticmethod
    def save_config(config):
        """保存配置到文件"""
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
            # logger.info(f"Configuration saved to {CONFIG_FILE}")
        except Exception as e:
            logger.error(f"Error saving configuration to {CONFIG_FILE}: {e}")
   
   