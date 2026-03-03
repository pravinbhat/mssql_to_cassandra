"""
Cassandra connector for Spark operations.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, List
import logging


class CassandraConnector:
    """Handle Cassandra database connections and operations using Spark."""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize Cassandra connector.

        Args:
            spark: SparkSession instance
            config: Cassandra configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)

        self.cassandra_config = config["cassandra"]
        self.keyspace = self.cassandra_config["keyspace"]

    def read_table(self, table_name: str, keyspace: str = None) -> DataFrame:
        """
        Read a table from Cassandra.

        Args:
            table_name: Name of the table
            keyspace: Keyspace name (uses default if not provided)

        Returns:
            Spark DataFrame containing table data
        """
        ks = keyspace or self.keyspace
        self.logger.info(f"Reading table: {ks}.{table_name}")

        df = (
            self.spark.read.format("org.apache.spark.sql.cassandra")
            .options(table=table_name, keyspace=ks)
            .load()
        )

        self.logger.info(f"Successfully read {df.count()} rows from {ks}.{table_name}")
        return df

    def write_table(
        self, df: DataFrame, table_name: str, keyspace: str = None, mode: str = "append"
    ) -> None:
        """
        Write DataFrame to Cassandra table.

        Args:
            df: Spark DataFrame to write
            table_name: Target table name
            keyspace: Keyspace name (uses default if not provided)
            mode: Write mode (append, overwrite, ignore, error)
        """
        ks = keyspace or self.keyspace
        self.logger.info(f"Writing to table: {ks}.{table_name} (mode: {mode})")

        write_options = self.config.get("write_options", {})

        # Get Spark connector settings for optimized writes
        spark_connector = self.cassandra_config.get("spark_connector", {})

        # Build write operation with all optimization options
        writer = df.write.format("org.apache.spark.sql.cassandra")
        
        # Basic options
        writer = writer.options(table=table_name, keyspace=ks)
        writer = writer.option(
            "confirm.truncate", str(write_options.get("confirm.truncate", True)).lower()
        )
        
        # Apply Spark connector optimization settings if available
        if "output.batch.size.rows" in spark_connector:
            writer = writer.option(
                "spark.cassandra.output.batch.size.rows",
                spark_connector["output.batch.size.rows"]
            )
        
        if "output.batch.size.bytes" in spark_connector:
            writer = writer.option(
                "spark.cassandra.output.batch.size.bytes",
                spark_connector["output.batch.size.bytes"]
            )
        
        if "output.concurrent.writes" in spark_connector:
            writer = writer.option(
                "spark.cassandra.output.concurrent.writes",
                spark_connector["output.concurrent.writes"]
            )
        
        if "output.throughput_mb_per_sec" in spark_connector:
            throughput = spark_connector["output.throughput_mb_per_sec"]
            # Only set if it's a valid number
            if isinstance(throughput, (int, float)) or (isinstance(throughput, str) and throughput.isdigit()):
                writer = writer.option(
                    "spark.cassandra.output.throughput_mb_per_sec",
                    str(throughput)
                )
        
        if "output.batch.grouping.key" in spark_connector:
            writer = writer.option(
                "spark.cassandra.output.batch.grouping.key",
                spark_connector["output.batch.grouping.key"]
            )
        
        # Execute write
        writer.mode(mode).save()


        self.logger.info(f"Successfully wrote data to {ks}.{table_name}")

    def create_keyspace(self, keyspace: str = None) -> None:
        """
        Create a keyspace in Cassandra.

        Args:
            keyspace: Keyspace name (uses default if not provided)
        """
        ks = keyspace or self.keyspace
        keyspace_config = self.config.get("keyspace", {})

        replication_strategy = keyspace_config.get("replication_strategy", "SimpleStrategy")
        replication_factor = keyspace_config.get("replication_factor", 1)
        durable_writes = keyspace_config.get("durable_writes", True)

        cql = f"""
        CREATE KEYSPACE IF NOT EXISTS {ks}
        WITH replication = {{
            'class': '{replication_strategy}',
            'replication_factor': {replication_factor}
        }}
        AND durable_writes = {str(durable_writes).lower()}
        """

        self.logger.info(f"Creating keyspace: {ks}")
        self.execute_cql(cql)
        self.logger.info(f"Keyspace {ks} created successfully")

    def create_table_from_dataframe(
        self,
        df: DataFrame,
        table_name: str,
        partition_key: List[str],
        clustering_keys: List[str] = None,
        keyspace: str = None,
    ) -> None:
        """
        Create a Cassandra table from DataFrame schema if it doesn't exist.

        Args:
            df: Source DataFrame with schema
            table_name: Name of the table
            partition_key: List of partition key columns
            clustering_keys: List of clustering key columns (optional)
            keyspace: Keyspace name (uses default if not provided)
        """
        import time
        
        ks = keyspace or self.keyspace

        # Check if table already exists
        if self.table_exists(table_name, ks):
            self.logger.info(f"Table {ks}.{table_name} already exists")
            return

        self.logger.info(f"Creating Cassandra table from DataFrame: {ks}.{table_name}")

        # Build schema from DataFrame
        schema_parts = []
        for field in df.schema.fields:
            # Map Spark types to Cassandra types
            spark_type = str(field.dataType)
            if "IntegerType" in spark_type:
                cass_type = "int"
            elif "LongType" in spark_type:
                cass_type = "bigint"
            elif "DoubleType" in spark_type or "FloatType" in spark_type:
                cass_type = "double"
            elif "StringType" in spark_type:
                cass_type = "text"
            elif "DateType" in spark_type:
                cass_type = "date"
            elif "TimestampType" in spark_type:
                cass_type = "timestamp"
            elif "BooleanType" in spark_type:
                cass_type = "boolean"
            else:
                cass_type = "text"  # Default fallback

            schema_parts.append(f"{field.name} {cass_type}")

        schema_str = ",\n            ".join(schema_parts)

        # Create table using the existing create_table method
        self.create_table(table_name, schema_str, partition_key, clustering_keys, ks)
        
        # Wait a moment for table metadata to propagate
        time.sleep(2)

    def create_table(
        self,
        table_name: str,
        schema: str,
        partition_key: List[str],
        clustering_keys: List[str] = None,
        keyspace: str = None,
    ) -> None:
        """
        Create a table in Cassandra.

        Args:
            table_name: Name of the table
            schema: Table schema definition
            partition_key: List of partition key columns
            clustering_keys: List of clustering key columns (optional)
            keyspace: Keyspace name (uses default if not provided)
        """
        ks = keyspace or self.keyspace

        # Build primary key clause
        if clustering_keys and len(clustering_keys) > 0:
            # Composite partition key with clustering columns
            if len(partition_key) > 1:
                primary_key = f"(({', '.join(partition_key)}), {', '.join(clustering_keys)})"
            else:
                primary_key = f"({partition_key[0]}, {', '.join(clustering_keys)})"
        else:
            # No clustering keys
            if len(partition_key) > 1:
                primary_key = f"(({', '.join(partition_key)}))"
            else:
                primary_key = partition_key[0]

        cql = f"""
        CREATE TABLE IF NOT EXISTS {ks}.{table_name} (
            {schema},
            PRIMARY KEY ({primary_key})
        )
        """

        self.logger.info(f"Creating table: {ks}.{table_name}")
        self.logger.debug(f"CQL: {cql}")
        self.execute_cql(cql)
        self.logger.info(f"Table {ks}.{table_name} created successfully")

    def execute_cql(self, cql: str) -> None:
        """
        Execute a CQL statement.

        Args:
            cql: CQL statement to execute
        """
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider

        contact_points = self.cassandra_config["contact_points"]
        port = self.cassandra_config["port"]

        # Create connection with optional authentication
        cluster_kwargs = {"contact_points": contact_points, "port": port}

        # Only add auth if username and password are provided
        username = self.cassandra_config.get("username")
        password = self.cassandra_config.get("password")
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster_kwargs["auth_provider"] = auth_provider

        cluster = Cluster(**cluster_kwargs)
        session = cluster.connect()

        try:
            session.execute(cql)
        finally:
            cluster.shutdown()

    def table_exists(self, table_name: str, keyspace: str = None) -> bool:
        """
        Check if a table exists in Cassandra.

        Args:
            table_name: Name of the table
            keyspace: Keyspace name (uses default if not provided)

        Returns:
            True if table exists, False otherwise
        """
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider

        ks = keyspace or self.keyspace

        contact_points = self.cassandra_config["contact_points"]
        port = self.cassandra_config["port"]

        # Create connection with optional authentication
        cluster_kwargs = {"contact_points": contact_points, "port": port}

        # Only add auth if username and password are provided
        username = self.cassandra_config.get("username")
        password = self.cassandra_config.get("password")
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster_kwargs["auth_provider"] = auth_provider

        cluster = Cluster(**cluster_kwargs)
        session = cluster.connect()

        try:
            query = f"""
            SELECT table_name
            FROM system_schema.tables
            WHERE keyspace_name = '{ks}' AND table_name = '{table_name}'
            """
            result = session.execute(query)
            return len(list(result)) > 0
        finally:
            cluster.shutdown()
