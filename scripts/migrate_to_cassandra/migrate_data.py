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
    spark_config = config["spark"]
    cassandra_conn = cassandra_config["cassandra"]["spark_connector"]

    builder = SparkSession.builder.appName(spark_config["app_name"]).master(spark_config["master"])

    # Add Spark configuration properties
    for key, value in spark_config["config"].items():
        builder = builder.config(key, value)

    # Add Cassandra connector configuration
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


def migrate_table(
    mssql: MSSQLConnector,
    cassandra: CassandraConnector,
    source_table: str,
    target_table: str,
    table_mapping: dict,
    logger,
):
    """
    Migrate a single table from MSSQL to Cassandra.

    Args:
        mssql: MSSQL connector instance
        cassandra: Cassandra connector instance
        source_table: Source table name (with schema)
        target_table: Target table name
        table_mapping: Table mapping configuration with partition/clustering keys
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
    df = mssql.read_table(table, schema)

    row_count = df.count()
    logger.info(f"Read {row_count} rows from {source_table}")

    if row_count == 0:
        logger.warning(f"No data found in {source_table}, skipping")
        return

    # Show schema
    logger.info(f"Source schema: {df.schema}")

    # Create table if it doesn't exist
    if not cassandra.table_exists(target_table):
        logger.info(f"Creating Cassandra table: {target_table}")

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

        # Get partition and clustering keys from mapping
        partition_key = table_mapping.get("partition_key", [])
        if isinstance(partition_key, str):
            partition_key = [partition_key]
        clustering_keys = table_mapping.get("clustering_keys", [])

        cassandra.create_table(target_table, schema_str, partition_key, clustering_keys)
        logger.info(f"Table {target_table} created successfully")
        # Wait a moment for table metadata to propagate
        time.sleep(2)
    else:
        logger.info(f"Table {target_table} already exists")

    # Write to Cassandra
    logger.info(f"Writing data to Cassandra: {target_table}")
    cassandra.write_table(df, target_table)

    logger.info(f"Successfully migrated {row_count} rows from {source_table} to {target_table}")


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

        # Migrate each table
        logger.info(f"Found {len(table_mappings)} tables to migrate")

        for mapping in table_mappings:
            source_table = mapping["source_table"]
            target_table = mapping["target_table"]

            try:
                migrate_table(mssql, cassandra, source_table, target_table, mapping, logger)
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
