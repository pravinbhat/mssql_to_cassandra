"""
Configuration loader utility for reading YAML configuration files.
"""

import yaml
from pathlib import Path
from typing import Dict, Any


class ConfigLoader:
    """Load and manage configuration files."""

    def __init__(self, config_dir: str = "config"):
        """
        Initialize the config loader.

        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = Path(config_dir)

    def load_config(self, config_file: str) -> Dict[str, Any]:
        """
        Load a YAML configuration file.

        Args:
            config_file: Name of the configuration file

        Returns:
            Dictionary containing configuration
        """
        config_path = self.config_dir / config_file

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        return config

    def load_mssql_config(self) -> Dict[str, Any]:
        """Load MSSQL configuration."""
        return self.load_config("mssql_config.yaml")

    def load_cassandra_config(self) -> Dict[str, Any]:
        """Load Cassandra configuration."""
        return self.load_config("cassandra_config.yaml")

    def load_spark_config(self) -> Dict[str, Any]:
        """Load Spark configuration."""
        return self.load_config("spark_config.yaml")

    def get_mssql_jdbc_url(self, config: Dict[str, Any] = None) -> str:
        """
        Build MSSQL JDBC URL from configuration.

        Args:
            config: MSSQL configuration dictionary (loads if not provided)

        Returns:
            JDBC connection URL
        """
        if config is None:
            config = self.load_mssql_config()

        mssql_config = config["mssql"]
        connection_config = mssql_config["connection"]

        jdbc_url = mssql_config["jdbc_url"].format(
            host=mssql_config["host"],
            port=mssql_config["port"],
            database=mssql_config["database"],
            encrypt=str(connection_config["encrypt"]).lower(),
            trustServerCertificate=str(connection_config["trustServerCertificate"]).lower(),
        )

        return jdbc_url

    def get_mssql_properties(self, config: Dict[str, Any] = None) -> Dict[str, str]:
        """
        Get MSSQL connection properties.

        Args:
            config: MSSQL configuration dictionary (loads if not provided)

        Returns:
            Dictionary of connection properties
        """
        if config is None:
            config = self.load_mssql_config()

        mssql_config = config["mssql"]

        properties = {
            "user": mssql_config["username"],
            "password": mssql_config["password"],
            "driver": mssql_config["driver"],
        }

        return properties
