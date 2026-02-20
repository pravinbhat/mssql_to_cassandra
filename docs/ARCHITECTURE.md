# Architecture Overview

This document describes the architecture and design decisions for the MSSQL to Cassandra migration project.

## System Architecture

```
┌────────────────────────────────────────────────────┐
│              Migration System                      │
├────────────────────────────────────────────────────┤
│                                                    │
│  ┌──────────────┐         ┌──────────────┐         │
│  │   CSV Files  │         │  MSSQL DB    │         │
│  │  (Sample     │         │  (Source)    │         │
│  │   Data)      │         │              │         │
│  └──────┬───────┘         └──────┬───────┘         │
│         │                        │                 │
│         │                        │                 │
│         ▼                        ▼                 │
│  ┌─────────────────────────────────────────┐       │
│  │         Apache Spark Engine             │       │
│  │  ┌───────────────────────────────────┐  │       │
│  │  │   Load Script (load_data.py)      │  │       │
│  │  │   - Read CSV files                │  │       │
│  │  │   - Transform data                │  │       │
│  │  │   - Write to MSSQL                │  │       │
│  │  └───────────────────────────────────┘  │       │
│  │                                         │       │
│  │  ┌───────────────────────────────────┐  │       │
│  │  │ Migration Script (migrate_data.py)│  │       │
│  │  │   - Read from MSSQL               │  │       │
│  │  │   - Transform data                │  │       │
│  │  │   - Write to Cassandra            │  │       │
│  │  └───────────────────────────────────┘  │       │
│  └─────────────────────────────────────────┘       │
│                        │                           │
│                        ▼                           │
│                 ┌──────────────┐                   │
│                 │  Cassandra   │                   │
│                 │  (Target)    │                   │
│                 └──────────────┘                   │
│                                                    │
└────────────────────────────────────────────────────┘
```

## Component Design

### 1. Configuration Layer (`config/`)

**Purpose**: Centralized configuration management

**Components**:
- `mssql_config.yaml`: MSSQL connection and table settings
- `cassandra_config.yaml`: Cassandra connection and table mappings
- `spark_config.yaml`: Spark session configuration

**Design Pattern**: Configuration as Code

### 2. Utility Layer (`src/utils/`)

**Purpose**: Reusable utility functions

**Components**:
- `config_loader.py`: YAML configuration loader
- `logger.py`: Logging setup and management

### 3. Connector Layer (`src/connectors/`)

**Purpose**: Database abstraction and connection management

**Components**:
- `mssql_connector.py`: MSSQL JDBC operations
- `cassandra_connector.py`: Cassandra Spark connector operations

**Key Features**:
- Abstraction over database-specific APIs
- Reusable read/write operations
- Connection pooling via Spark
- Error handling and logging

### 4. Script Layer (`scripts/`)

**Purpose**: Executable migration workflows

**Components**:
- `load_mssql/load_data.py`: Load CSV data into MSSQL
- `migrate_to_cassandra/migrate_data.py`: Migrate MSSQL to Cassandra

## Data Flow

### Load Flow (CSV → MSSQL)

```
CSV Files → Spark DataFrame → MSSQL Connector → MSSQL Database
```

1. **Read**: Spark reads CSV files with schema inference
2. **Transform**: Optional data transformations
3. **Write**: JDBC batch write to MSSQL tables

### Migration Flow (MSSQL → Cassandra)

```
MSSQL Database → MSSQL Connector → Spark DataFrame → 
Cassandra Connector → Cassandra Database
```

1. **Read**: JDBC read from MSSQL tables
2. **Transform**: Schema mapping and data type conversion
3. **Write**: Cassandra Spark connector batch write

## Design Decisions

### 1. Why Apache Spark?

- Distributed processing for large datasets
- Built-in JDBC support for MSSQL
- Native Cassandra connector
- Fault tolerance and retry mechanisms
- Scalable from local to cluster mode

### 2. Configuration Management using YAML files

- Human-readable format
- Easy to version control
- Supports complex nested structures
- No code changes for different environments

### 3. Table Mapping as configuration

- Clear documentation
- Flexible partition key selection
- Support for different table structures
- Easy to modify without code changes

### 4. Error Handling Strategy

- Comprehensive logging at each step
- Exception propagation with context
- Transaction-like behavior (all or nothing per table)

## Scalability Considerations

### Horizontal Scaling

**Spark Cluster Mode**:
```yaml
spark:
  master: "spark://master:7077"
  config:
    spark.executor.instances: 10
    spark.executor.cores: 4
    spark.executor.memory: "8g"
```

### Vertical Scaling

**Memory Optimization**:
- Adjust `spark.driver.memory` and `spark.executor.memory`
- Tune `spark.sql.shuffle.partitions`
- Use appropriate batch sizes

### Data Partitioning

**Spark Partitioning**:
- Managed via configuration
- Future improvement: Implement repartition() or coalesce() to optimize data distribution for large datasets

**Cassandra Partitioning**:
- Design partition keys for even distribution
- Use clustering keys for time-series data

## Performance Optimization

### 1. Batch Processing

- MSSQL: `batchsize` parameter (default: 10000)
- Cassandra: `output.batch.size.rows` (default: 1000)

### 2. Parallel Processing

- Multiple Spark executors
- Concurrent writes to Cassandra
- Partitioned reads from MSSQL

### Metrics
- Spark UI (http://localhost:4040)
- Row counts and processing times
- Error rates and retry attempts
