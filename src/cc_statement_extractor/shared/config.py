import os
import yaml

CONFIG_PATH = os.path.join(os.getcwd(), "config.yaml")

class Config:
    def __init__(self, config_path: str = None):
        self.config_path = config_path or CONFIG_PATH
        self.config = self._load_configs()

    def _load_configs(self) -> dict:
        if not os.path.exists(self.config_path):
            return {}
        with open(self.config_path, "r") as f:
            return yaml.safe_load(f) or {}

    def get(self, key: str, default: any = None) -> any:
        """
        Get a setting value by key. Supports dot notation for nested keys.

        Args:
            key (str): The key to get the value for (e.g., 'huggingface.model').
            default (any): The default value to return if the key is not found.

        Returns:
            any: The value for the key, or the default value if the key is not found.
        """
        parts = key.split(".")
        val = self.config
        for part in parts:
            if isinstance(val, dict) and part in val:
                val = val[part]
            else:
                return default
        return val

    def set(self, key: str, value: any) -> None:
        """
        Set a setting value by key. Supports dot notation for nested keys.

        Args:
            key (str): The key to set the value for (e.g., 'huggingface.model').
            value (any): The value to set for the key.
        """
        parts = key.split(".")
        val = self.config
        for part in parts[:-1]:
            if part not in val or not isinstance(val[part], dict):
                val[part] = {}
            val = val[part]
        val[parts[-1]] = value

    def save(self) -> None:
        """
        Save the settings to the config file.
        """
        with open(self.config_path, "w") as f:
            yaml.dump(self.config, f)