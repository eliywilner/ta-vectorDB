import os
import logging
import configparser

class Config:
    def __init__(self, config_file_name: str = 'config.ini'):
        self.config_file_name = config_file_name
        self.base_path = os.path.abspath(os.path.dirname(__file__))
        self.config_file_path = os.path.join(self.base_path, self.config_file_name)
    def get_config_path(self):
        if os.path.exists(self.config_file_path):
            return self.config_file_path
        else:
            logging.error(f"Config file not found at {self.config_file_path}")
            raise FileNotFoundError(f"Config file not found at {self.config_file_path}")
    def get_configurations(self, section: str) -> dict:
        config = configparser.ConfigParser()
        config.read(self.get_config_path())
        if section in config:
            return dict(config[section])
        else:
            raise ValueError(f"Section '{section}' not found in {self.config_file_path()}")
            return {}
