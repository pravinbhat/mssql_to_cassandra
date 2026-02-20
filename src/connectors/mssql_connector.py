"""
MSSQL connector for Spark operations.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Optional
import logging


class MSSQLConnector:
    """Handle MSSQL database connections and operations using Spark."""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize MSSQL connector.

        Args:
            spark: SparkSession instance
            config: MSSQL configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Build JDBC URL
        mssql_config = config["mssql"]
        connection_config = mssql_config["connection"]

        self.jdbc_url = mssql_config["jdbc_url"].format(
            host=mssql_config["host"],
            port=mssql_config["port"],
            database=mssql_config["database"],
            encrypt=str(connection_config["encrypt"]).lower(),
            trustServerCertificate=str(connection_config["trustServerCertificate"]).lower(),
        )

        self.properties = {
            "user": mssql_config["username"],
            "password": mssql_config["password"],
            "driver": mssql_config["driver"],
        }

    def read_table(self, table_name: str, schema: str = "dbo") -> DataFrame:
        """
        Read a table from MSSQL.

        Args:
            table_name: Name of the table
            schema: Database schema (default: dbo)

        Returns:
            Spark DataFrame containing table data
        """
        full_table_name = f"{schema}.{table_name}"
        self.logger.info(f"Reading table: {full_table_name}")

        df = self.spark.read.jdbc(
            url=self.jdbc_url, table=full_table_name, properties=self.properties
        )

        self.logger.info(f"Successfully read {df.count()} rows from {full_table_name}")
        return df

    def write_table(
        self, df: DataFrame, table_name: str, schema: str = "dbo", mode: str = "overwrite"
    ) -> None:
        """
        Write DataFrame to MSSQL table.

        Args:
            df: Spark DataFrame to write
            table_name: Target table name
            schema: Database schema (default: dbo)
            mode: Write mode (overwrite, append, ignore, error)
        """
        full_table_name = f"{schema}.{table_name}"
        self.logger.info(f"Writing to table: {full_table_name} (mode: {mode})")

        write_options = self.config.get("write_options", {})

        df.write.jdbc(
            url=self.jdbc_url,
            table=full_table_name,
            mode=mode,
            properties={
                **self.properties,
                "batchsize": str(write_options.get("batchsize", 10000)),
                "isolationLevel": write_options.get("isolationLevel", "READ_COMMITTED"),
            },
        )

        self.logger.info(f"Successfully wrote data to {full_table_name}")
