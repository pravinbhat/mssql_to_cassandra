"""
Unit tests for ConfigLoader utility.
"""

import pytest
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils import ConfigLoader


class TestConfigLoader:
    """Test cases for ConfigLoader class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.config_loader = ConfigLoader()

    def test_load_mssql_config(self):
        """Test loading MSSQL configuration."""
        config = self.config_loader.load_mssql_config()

        assert "mssql" in config
        assert "host" in config["mssql"]
        assert "port" in config["mssql"]
        assert "database" in config["mssql"]
        assert "username" in config["mssql"]
        assert "password" in config["mssql"]

    def test_load_cassandra_config(self):
        """Test loading Cassandra configuration."""
        config = self.config_loader.load_cassandra_config()

        assert "cassandra" in config
        assert "contact_points" in config["cassandra"]
        assert "port" in config["cassandra"]
        assert "keyspace" in config["cassandra"]

    def test_load_spark_config(self):
        """Test loading Spark configuration."""
        config = self.config_loader.load_spark_config()

        assert "spark" in config
        assert "app_name" in config["spark"]
        assert "master" in config["spark"]
        assert "config" in config["spark"]

    def test_get_mssql_jdbc_url(self):
        """Test JDBC URL generation."""
        config = self.config_loader.load_mssql_config()
        jdbc_url = self.config_loader.get_mssql_jdbc_url(config)

        assert jdbc_url.startswith("jdbc:sqlserver://")
        assert "databaseName=" in jdbc_url
        assert "encrypt=" in jdbc_url

    def test_get_mssql_properties(self):
        """Test MSSQL properties generation."""
        config = self.config_loader.load_mssql_config()
        properties = self.config_loader.get_mssql_properties(config)

        assert "user" in properties
        assert "password" in properties
        assert "driver" in properties
        assert properties["driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    def test_config_file_not_found(self):
        """Test handling of missing configuration file."""
        with pytest.raises(FileNotFoundError):
            self.config_loader.load_config("nonexistent.yaml")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
