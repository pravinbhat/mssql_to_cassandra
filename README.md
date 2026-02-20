# MSSQL to Cassandra Migration Project

This project provides Spark-based scripts to migrate data from MSSQL Server to Apache Cassandra. It includes:
- **Migration script**: Automated data migration from MSSQL to Cassandra with schema mapping
- **Data loader**: Scripts to load sample data into MSSQL Server for testing

## Prerequisites

- **Java 17+** (required for PySpark 4.x)
- Apache Spark 3.x+
- Python 3.12+
- MSSQL Server
- Apache Cassandra
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

3. **Install project dependencies**:
   ```bash
   uv sync
   ```

4. **Configure database connections** in `config/` directory

5. **Place your test data** in `data/sample/`

## Usage

**Important**: All commands must be run with Java 17+ for PySpark 4.x compatibility.

### Using the helper script (Recommended)

#### 1. Load Sample Data into MSSQL
Load test data from CSV files into MSSQL Server:
```bash
./run.sh uv run python scripts/load_mssql/load_data.py
```

#### 2. Migrate Data from MSSQL to Cassandra
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

**Command-line Options:**
- `--mssql-config`: Path to MSSQL configuration file (default: `config/mssql_config.yaml`)
- `--cassandra-config`: Path to Cassandra configuration file (default: `config/cassandra_config.yaml`)
- `--create-keyspace`: Create the Cassandra keyspace if it doesn't exist
- `--log-level`: Set logging level (DEBUG, INFO, WARN, ERROR; default: INFO)


## Configuration

Edit the YAML files in the `config/` directory to match your environment:
- Database connection details for Cassandra and MSSQL Server
- Authentication credentials
- Spark settings
- Table mappings from MSSQL Server to Cassandra

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


## License

MIT License