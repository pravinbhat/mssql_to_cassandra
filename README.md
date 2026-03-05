# MSSQL to Cassandra Migration Project

This project provides Spark-based scripts to migrate data from MSSQL Server to Cassandra. It includes:
- **Migration script**: Automated data migration from MSSQL to Cassandra with schema mapping
- **Data loader**: Scripts to load sample data into MSSQL Server for testing
- **Secure Connection Support**: Connect securely using environment variables

> **Note**: This tool supports all Apache Cassandra-compatible (CQL based) databases, including Apache Cassandra, DataStax Enterprise (DSE), DataStax Hyper Converged Database (HCD), and DataStax Astra DB.

> 📖 **Documentation**:
> - [Architecture Documentation](docs/ARCHITECTURE.md) - System design and components
> - [Optimization Guide](docs/OPTIMIZATION_GUIDE.md) - Performance tuning for large datasets

## Prerequisites

- **Java 17+** (required for PySpark 4.x)
- Apache Spark 3.x+
- Python 3.12+
- MSSQL Server
- **Target Database**: Cassandra or any CQL-compatible database
- [UV](https://docs.astral.sh/uv/) - Fast Python package installer and resolver

## Setup

1. **Install Java 17+** (if not already installed):
   
   Using SDKMAN (recommended):
   ```bash
   sdk install java 17.0.16-tem
   sdk use java 17.0.16-tem
   ```
   
   Configure Java, Python and Spark env variables (see .env.example for details)


2. **Install UV** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Download required JDBC drivers**:
   ```bash
   ./scripts/download_jars.sh
   ```
   
   This script downloads:
   - **MSSQL JDBC Driver** (v12.8.1.jre11) - Required for connecting Spark to MSSQL Server
   - **Cassandra Spark Connector** (v3.5.0) - Required for writing data to Cassandra
   
   The JARs are downloaded to the `jars/` directory and are automatically used by Spark.

4. **Install project dependencies**:
   ```bash
   uv sync
   ```

5. **Configure database connections** in `config/` directory

6. **Place your test data** in `data/sample/`

## Using the helper script

**Important**: All commands must be run with Java 17+ for PySpark 4.x compatibility.

#### 1. Generate Sample Data (Optional, for testing only)
Generate sample CSV files for testing:
```bash
python scripts/generate_sample_data.py [customers] [products] [orders] [max_items_per_order]
```

**Arguments** (all optional, defaults to 10):
- `customers`: Number of customers to generate
- `products`: Number of products to generate
- `orders`: Number of orders to generate
- `max_items_per_order`: Maximum items per order

**Example:**
```bash
# Generate default data (10 of each)
python scripts/generate_sample_data.py

# Generate custom amounts
python scripts/generate_sample_data.py 100 50 200 5
```

Generates four CSV files in `data/sample/`:
- `customers.csv` - Customer information
- `products.csv` - Product catalog
- `orders.csv` - Order records
- `order_details.csv` - Order line items

#### 2. Load Sample Data into MSSQL (Optional, for testing only)
Load test data from CSV files into MSSQL Server:
```bash
./run.sh uv run python scripts/load_mssql/load_data.py
```

CSV files should have:
- Header row with column names
- Comma-separated values
- Proper data types (will be inferred by Spark)

Example:
```csv
customer_id,first_name,last_name,email
1,John,Doe,john@example.com
2,Jane,Smith,jane@example.com
```


## Migrate Data from MSSQL to Cassandra

Migrate data from MSSQL Server to Cassandra using the configured table mappings:

```bash
# Basic migration (assumes keyspace exists)
./run.sh uv run python scripts/migrate_to_cassandra/migrate_data.py

# Create keyspace if it doesn't exist
./run.sh uv run python scripts/migrate_to_cassandra/migrate_data.py --create-keyspace

# Use custom configuration files
./run.sh uv run python scripts/migrate_to_cassandra/migrate_data.py \
  --mssql-config config/mssql_config.yaml \
  --cassandra-config config/cassandra_config.yaml

# Enable debug logging
./run.sh uv run python scripts/migrate_to_cassandra/migrate_data.py --log-level DEBUG
```

**Migration Features:**
- Automatically creates Cassandra tables based on MSSQL schema
- Maps MSSQL data types to appropriate Cassandra types
- Supports custom partition and clustering keys per table
- Handles multiple table migrations in a single run
- Provides detailed logging of the migration process
- Continues migration even if individual tables fail
- **Performance optimizations** for large datasets (dynamic repartitioning, caching, batch writes)

> 💡 **Performance Tip**: For large datasets, see the [Optimization Guide](docs/OPTIMIZATION_GUIDE.md) for tuning recommendations.

**Command-line Options:**
- `--mssql-config`: Path to MSSQL configuration file (default: `config/mssql_config.yaml`)
- `--cassandra-config`: Path to Cassandra configuration file (default: `config/cassandra_config.yaml`)
- `--create-keyspace`: Create the Cassandra keyspace if it doesn't exist
- `--log-level`: Set logging level (DEBUG, INFO, WARN, ERROR; default: INFO)


## Configuration

Edit the YAML files in the `config/` directory to match your environment:
- Database connection details for Cassandra/AstraDB and MSSQL Server
- Authentication credentials
- Spark settings
- Table mappings from MSSQL Server to Cassandra

### Cassandra Connection Modes

The tool supports two connection modes:

1. **Standard Mode** (default):
   ```yaml
   cassandra:
     connection_mode: "standard"
     contact_points: ["localhost"]
     port: 9042
     keyspace: "migration_db"
   ```

2. **Astra Mode** (for DataStax Astra DB):
   ```yaml
   cassandra:
     connection_mode: "astra"
     astra:
       secure_connect_bundle: "/path/to/secure-connect-db.zip"
       client_id: "your-client-id"
       client_secret: "your-client-secret"
       keyspace: "migration_db"
   ```
   
   > **Note**: When using `connection_mode: "astra"`, the keyspace must be created beforehand using the Astra DB UI. The migration tool cannot create keyspaces programmatically in Astra mode.

### Secure Connection with Environment Variables

For better security, use environment variables instead of hardcoding credentials:

```bash
# Set environment variables
export ASTRA_SCB_PATH="/path/to/secure-connect-db.zip"
export ASTRA_CLIENT_ID="your-client-id"
export ASTRA_CLIENT_SECRET="your-client-secret"
```

Then reference them in your configuration:

```yaml
cassandra:
  connection_mode: "astra"
  astra:
    secure_connect_bundle: "${ASTRA_SCB_PATH}"
    client_id: "${ASTRA_CLIENT_ID}"
    client_secret: "${ASTRA_CLIENT_SECRET}"
    keyspace: "migration_db"
```

## Development

### Adding Dependencies
```bash
# Add a production dependency
uv add package-name

# Add a development dependency
uv add --dev package-name
```

### Updating Dependencies
```bash
uv sync --upgrade
```


## Troubleshooting

### Java Gateway Error
If you see `PySparkRuntimeError: [JAVA_GATEWAY_EXITED]`, this means PySpark cannot find Java 17+.

**Solution**:
1. Verify Java 17+ is installed: `java -version`
2. Use the helper script: `./run.sh uv run python scripts/...`


## Testing

Run tests with:
```bash
uv run pytest tests/
```


## References

- [Apache Cassandra Documentation](https://cassandra.apache.org/doc/latest/)
- [DataStax Astra DB Documentation](https://docs.datastax.com/en/astra/docs/)
- [Secure Connect Bundle Guide](https://docs.datastax.com/en/astra/docs/connecting-to-astra-databases.html)
- [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector)
- [DataStax Enterprise (DSE)](https://docs.datastax.com/en/dse/index.html)

## License

MIT License