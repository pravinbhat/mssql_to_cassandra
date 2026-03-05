"""
Migrate data from MSSQL Server to Cassandra using Spark.
"""

import sys
import argparse
import time
from pathlib import Path
from pyspark.sql import SparkSession

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils import ConfigLoader, setup_logger
from src.connectors import MSSQLConnector, CassandraConnector


def create_spark_session(config: dict, cassandra_config: dict) -> SparkSession:
    """Create and configure Spark session with Cassandra connector."""
    import os
    
    spark_config = config["spark"]
    cassandra_conn = cassandra_config["cassandra"]["spark_connector"]
    cassandra_main_config = cassandra_config["cassandra"]
    connection_mode = cassandra_main_config.get("connection_mode", "standard")

    builder = SparkSession.builder.appName(spark_config["app_name"]).master(spark_config["master"])

    # Add Spark configuration properties
    for key, value in spark_config["config"].items():
        builder = builder.config(key, value)

    # Configure based on connection mode
    if connection_mode == "astra":
        # AstraDB configuration using Secure Connect Bundle
        astra_config = cassandra_main_config.get("astra", {})
        scb_path = astra_config.get("secure_connect_bundle")
        client_id = astra_config.get("client_id")
        client_secret = astra_config.get("client_secret")

        if not scb_path:
            raise ValueError("secure_connect_bundle path is required for AstraDB connection")
        
        if not Path(scb_path).exists():
            raise FileNotFoundError(f"Secure Connect Bundle not found at: {scb_path}")

        if not client_id or not client_secret:
            raise ValueError("client_id and client_secret are required for AstraDB connection")

        # Distribute SCB file to all executors using spark.files
        # This makes the file available on all worker nodes
        scb_absolute_path = os.path.abspath(scb_path)
        builder = builder.config("spark.files", scb_absolute_path)
        
        # Get just the filename for the distributed file
        scb_filename = os.path.basename(scb_path)
        
        # Set AstraDB-specific Spark configurations
        # Use the filename only since Spark will place it in the working directory of each executor
        builder = builder.config("spark.cassandra.connection.config.cloud.path", scb_filename)
        builder = builder.config("spark.cassandra.auth.username", client_id)
        builder = builder.config("spark.cassandra.auth.password", client_secret)
        
        # Add other Cassandra connector configurations (excluding connection.host and connection.port)
        for key, value in cassandra_conn.items():
            if key not in ["connection.host", "connection.port"]:
                builder = builder.config(f"spark.cassandra.{key}", value)
    else:
        # Standard Cassandra configuration
        for key, value in cassandra_conn.items():
            builder = builder.config(f"spark.cassandra.{key}", value)

    # Add JAR files
    if "jars" in spark_config:
        jars = ",".join(spark_config["jars"])
        builder = builder.config("spark.jars", jars)

    # Add packages
    if "packages" in spark_config:
        packages = ",".join(spark_config["packages"])
        builder = builder.config("spark.jars.packages", packages)

    return builder.getOrCreate()

def optimize_dataframe(
    df,
    row_count: int,
    partition_keys: list,
    optimization_config: dict,
    logger,
):
    """
    Optimize DataFrame for large dataset processing.

    Args:
        df: Input DataFrame
        row_count: Number of rows in DataFrame
        partition_keys: Partition key columns for Cassandra
        optimization_config: Optimization configuration
        logger: Logger instance

    Returns:
        Optimized DataFrame
    """
    # Get optimization parameters
    enable_repartition = optimization_config.get("enable_repartition", True)
    enable_cache = optimization_config.get("enable_cache", True)
    repartition_threshold = optimization_config.get("repartition_threshold", 100000)
    target_partitions = optimization_config.get("target_partitions", None)
    rows_per_partition = optimization_config.get("rows_per_partition", 50000)

    logger.info(f"Optimizing DataFrame with {row_count} rows")

    # Repartition for large datasets
    if enable_repartition and row_count >= repartition_threshold:
        if target_partitions:
            num_partitions = target_partitions
        else:
            # Calculate optimal partitions based on rows per partition
            num_partitions = max(1, row_count // rows_per_partition)

        logger.info(f"Repartitioning to {num_partitions} partitions for better parallelism")

        # Repartition by partition keys if available for better Cassandra write distribution
        if partition_keys and len(partition_keys) > 0:
            logger.info(f"Repartitioning by partition keys: {partition_keys}")
            df = df.repartition(num_partitions, *partition_keys)
        else:
            df = df.repartition(num_partitions)

        logger.info(f"Current partitions after repartition: {df.rdd.getNumPartitions()}")

    # Coalesce for small datasets to reduce overhead
    elif row_count < repartition_threshold:
        current_partitions = df.rdd.getNumPartitions()
        if current_partitions > 10:
            # Reduce partitions for small datasets
            optimal_partitions = max(1, min(10, row_count // 10000))
            logger.info(
                f"Coalescing from {current_partitions} to {optimal_partitions} partitions for small dataset"
            )
            df = df.coalesce(optimal_partitions)

    # Cache DataFrame if enabled (useful for multiple operations)
    if enable_cache:
        logger.info("Caching DataFrame in memory for faster access")
        df = df.cache()

    return df

def migrate_table(
    mssql: MSSQLConnector,
    cassandra: CassandraConnector,
    source_table: str,
    target_table: str,
    table_mapping: dict,
    optimization_config: dict,
    logger,
):
    """
    Migrate a single table from MSSQL to Cassandra with optimizations.
    Args:
        mssql: MSSQL connector instance
        cassandra: Cassandra connector instance
        source_table: Source table name (with schema)
        target_table: Target table name
        table_mapping: Table mapping configuration with partition/clustering keys
        optimization_config: Optimization configuration
        logger: Logger instance
    """
    logger.info(f"Migrating {source_table} -> {target_table}")

    # Extract schema and table name
    if "." in source_table:
        schema, table = source_table.split(".")
    else:
        schema, table = "dbo", source_table

    # Read from MSSQL
    logger.info(f"Reading data from MSSQL: {source_table}")
    start_time = time.time()
    df = mssql.read_table(table, schema)

    row_count = df.count()
    read_time = time.time() - start_time
    logger.info(f"Read {row_count} rows from {source_table} in {read_time:.2f} seconds")

    if row_count == 0:
        logger.warning(f"No data found in {source_table}, skipping")
        return

    # Show schema
    logger.info(f"Source schema: {df.schema}")

    # Get partition keys for optimization
    partition_key = table_mapping.get("partition_key", [])
    if isinstance(partition_key, str):
        partition_key = [partition_key]

    # Optimize DataFrame for large datasets
    df = optimize_dataframe(df, row_count, partition_key, optimization_config, logger)

    # Get clustering keys from mapping
    clustering_keys = table_mapping.get("clustering_keys", [])

    # Create table if it doesn't exist (using CassandraConnector method)
    cassandra.create_table_from_dataframe(df, target_table, partition_key, clustering_keys)

    # Write to Cassandra
    logger.info(f"Writing data to Cassandra: {target_table}")
    write_start_time = time.time()
    cassandra.write_table(df, target_table)
    write_time = time.time() - write_start_time

    # Unpersist cached DataFrame to free memory
    if optimization_config.get("enable_cache", True):
        df.unpersist()
        logger.info("Unpersisted cached DataFrame")

    total_time = time.time() - start_time
    logger.info(
        f"Successfully migrated {row_count} rows from {source_table} to {target_table}"
    )
    logger.info(
        f"Performance: Read={read_time:.2f}s, Write={write_time:.2f}s, Total={total_time:.2f}s"
    )
    logger.info(f"Throughput: {row_count / total_time:.0f} rows/sec")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Migrate data from MSSQL to Cassandra")
    parser.add_argument(
        "--mssql-config",
        default="config/mssql_config.yaml",
        help="Path to MSSQL configuration file",
    )
    parser.add_argument(
        "--cassandra-config",
        default="config/cassandra_config.yaml",
        help="Path to Cassandra configuration file",
    )
    parser.add_argument(
        "--create-keyspace",
        action="store_true",
        help="Create Cassandra keyspace if it does not exist",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARN", "ERROR"],
        help="Logging level",
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logger(
        "migrate_cassandra",
        log_file="logs/migrate_cassandra.log",
        level=args.log_level,
        console=True,
    )

    logger.info("Starting MSSQL to Cassandra migration process")

    try:
        # Load configurations
        config_loader = ConfigLoader()
        mssql_config = config_loader.load_mssql_config()
        cassandra_config = config_loader.load_cassandra_config()
        spark_config = config_loader.load_spark_config()

        # Create Spark session
        logger.info("Creating Spark session with Cassandra connector")
        spark = create_spark_session(spark_config, cassandra_config)

        # Initialize connectors
        logger.info("Initializing database connectors")
        mssql = MSSQLConnector(spark, mssql_config)
        cassandra = CassandraConnector(spark, cassandra_config)

        # Create keyspace if requested
        if args.create_keyspace:
            logger.info("Creating Cassandra keyspace")
            cassandra.create_keyspace()

        # Get table mappings
        table_mappings = cassandra_config.get("table_mappings", [])

        if not table_mappings:
            logger.error("No table mappings found in configuration")
            sys.exit(1)

        # Get optimization configuration
        optimization_config = cassandra_config.get("optimization", {})
        logger.info(f"Optimization settings: {optimization_config}")

        # Migrate each table
        logger.info(f"Found {len(table_mappings)} tables to migrate")

        for mapping in table_mappings:
            source_table = mapping["source_table"]
            target_table = mapping["target_table"]

            try:
                migrate_table(mssql, cassandra, source_table, target_table, mapping, optimization_config, logger)
            except Exception as e:
                logger.error(f"Error migrating {source_table}: {str(e)}", exc_info=True)
                # Continue with next table
                continue

        logger.info("Migration completed successfully")

    except Exception as e:
        logger.error(f"Error during migration: {str(e)}", exc_info=True)
        sys.exit(1)

    finally:
        if "spark" in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
