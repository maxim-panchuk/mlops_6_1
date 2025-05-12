import configparser
from pathlib import Path
from typing import Any, Dict

class Config:
    def __init__(
        self,
        config_path: str = 'config.ini'
    ):
        self.config = configparser.ConfigParser()
        self.config_path = Path(config_path)
        self._load_config()

    def _load_config(self) -> None:
        if not self.config_path.exists():
            raise FileNotFoundError(f'config file {self.config_path} not found')
        self.config.read(self.config_path)

    def get_logging_config(self) -> Dict[str, Any]:
        return {
            "level": self.config.get("logging", "level")
        }
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        return {
            "path_to_csv": self.config.get("pipeline", "path_to_csv")
        }
    
    def get_spark_config(self) -> Dict[str, Any]:
        return {
            "spark.driver.memory": self.config.get("spark", "spark.driver.memory")
        }