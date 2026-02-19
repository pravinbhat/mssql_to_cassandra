"""
Load test data into MSSQL Server using Spark.
"""

import sys
import argparse
from pathlib import Path
from pyspark.sql import SparkSession

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils import ConfigLoader, setup_logger
from src.connectors import MSSQLConnector


def create_spark_session(config: dict) -> SparkSession:
    """Create and configure Spark session."""
    spark_config = config["spark"]

    builder = SparkSession.builder.appName(spark_config["app_name"]).master(spark_config["master"])

    # Add configuration properties
    for key, value in spark_config["config"].items():
        builder = builder.config(key, value)

    # Add packages if specified
    if "packages" in spark_config and spark_config["packages"]:
        packages = ",".join(spark_config["packages"])
        builder = builder.config("spark.jars.packages", packages)

    # Add local JARs if specified
    if "jars" in spark_config and spark_config["jars"]:
        jars = ",".join(spark_config["jars"])
        builder = builder.config("spark.jars", jars)

    return builder.getOrCreate()


def load_sample_data(spark: SparkSession, data_dir: Path):
    """
    Load sample data from CSV files.

    Args:
        spark: SparkSession instance
        data_dir: Directory containing sample data files
    """
    dataframes = {}

    # Load customers
    customers_path = data_dir / "customers.csv"
    if customers_path.exists():
        dataframes["customers"] = spark.read.csv(str(customers_path), header=True, inferSchema=True)

    # Load orders
    orders_path = data_dir / "orders.csv"
    if orders_path.exists():
        dataframes["orders"] = spark.read.csv(str(orders_path), header=True, inferSchema=True)

    # Load products
    products_path = data_dir / "products.csv"
    if products_path.exists():
        dataframes["products"] = spark.read.csv(str(products_path), header=True, inferSchema=True)

    return dataframes


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Load test data into MSSQL Server")
    parser.add_argument(
        "--config", default="config/mssql_config.yaml", help="Path to MSSQL configuration file"
    )
    parser.add_argument(
        "--data-dir", default="data/sample", help="Directory containing sample data files"
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
        "load_mssql", log_file="logs/load_mssql.log", level=args.log_level, console=True
    )

    logger.info("Starting MSSQL data loading process")

    try:
        # Load configurations
        config_loader = ConfigLoader()
        mssql_config = config_loader.load_mssql_config()
        spark_config = config_loader.load_spark_config()

        # Create Spark session
        logger.info("Creating Spark session")
        spark = create_spark_session(spark_config)

        # Initialize MSSQL connector
        logger.info("Initializing MSSQL connector")
        mssql = MSSQLConnector(spark, mssql_config)

        # Load sample data
        data_dir = Path(args.data_dir)
        logger.info(f"Loading sample data from {data_dir}")
        dataframes = load_sample_data(spark, data_dir)

        if not dataframes:
            logger.warning("No data files found in the sample directory")
            return

        # Write data to MSSQL
        write_mode = mssql_config.get("write_options", {}).get("mode", "overwrite")

        for table_name, df in dataframes.items():
            logger.info(f"Writing {df.count()} rows to table: {table_name}")
            mssql.write_table(df, table_name, mode=write_mode)
            logger.info(f"Successfully loaded data into {table_name}")

        logger.info("Data loading completed successfully")

    except Exception as e:
        logger.error(f"Error during data loading: {str(e)}", exc_info=True)
        sys.exit(1)

    finally:
        if "spark" in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
