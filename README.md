# MSSQL to Cassandra Migration Project

This project provides Spark-based scripts to load test data into MSSQL Server and migrate that data to Apache Cassandra.

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

### Option 1: Using the helper script (Recommended)
```bash
# Load data into MSSQL
./run.sh uv run python scripts/load_mssql/load_data.py

```


## Configuration

Edit the YAML files in the `config/` directory to match your environment:
- Database connection strings
- Authentication credentials
- Spark settings
- Table mappings

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