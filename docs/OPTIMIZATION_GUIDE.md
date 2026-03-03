# Spark Optimization Guide for Large Dataset Migration

This guide explains the Spark optimizations implemented in the MSSQL to Cassandra migration tool to improve performance for large datasets.

> 📖 **Related Documentation**:
> - [README](../README.md) - Setup and usage instructions
> - [Architecture](ARCHITECTURE.md) - System design and component overview

## Overview

The migration script now includes several Spark optimization techniques:

1. **Dynamic Repartitioning** - Distributes data across partitions for better parallelism
2. **DataFrame Caching** - Caches data in memory for faster access
3. **Partition-based Distribution** - Repartitions by Cassandra partition keys for optimal writes
4. **Batch Write Optimization** - Configures Cassandra connector for efficient batch writes
5. **Adaptive Coalescing** - Reduces partitions for small datasets to minimize overhead

## Performance Improvements

- Parallel partition processing
- In-memory caching
- Optimized batch writes
- Better resource utilization

## Configuration

### 1. Cassandra Configuration (`config/cassandra_config.yaml`)

```yaml
cassandra:
  spark_connector:
    # Write optimization settings
    output.batch.size.rows: 1000          # Rows per batch
    output.batch.size.bytes: 1048576      # 1MB per batch
    output.concurrent.writes: 5           # Parallel writes
    output.throughput_mb_per_sec: 512     # MB/sec (high value for max speed)
    output.batch.grouping.key: partition  # Group by partition key

optimization:
  enable_repartition: true                # Enable repartitioning
  enable_cache: true                      # Enable DataFrame caching
  repartition_threshold: 100000           # Min rows for repartitioning
  target_partitions: null                 # Auto-calculate partitions
  rows_per_partition: 50000               # Rows per partition
```

### 2. Spark Configuration (`config/spark_config.yaml`)

```yaml
spark:
  config:
    spark.sql.shuffle.partitions: 200
    spark.default.parallelism: 8
    spark.sql.adaptive.enabled: true
    spark.sql.adaptive.coalescePartitions.enabled: true
```

## Optimization Strategies by Dataset Size

### Small Datasets (< 100K rows)
```yaml
optimization:
  enable_repartition: false  # Coalesce will handle it
  enable_cache: false        # Not needed for small data
  repartition_threshold: 100000
```

**Behavior**: Automatically coalesces to fewer partitions (1-10) to reduce overhead.

### Medium Datasets (100K - 1M rows)
```yaml
optimization:
  enable_repartition: true
  enable_cache: true
  repartition_threshold: 100000
  rows_per_partition: 50000  # Results in 2-20 partitions
```

**Behavior**: Repartitions for parallelism, caches for performance.

### Large Datasets (1M+ rows)
```yaml
optimization:
  enable_repartition: true
  enable_cache: true
  repartition_threshold: 100000
  rows_per_partition: 50000  # Results in 20+ partitions
  # OR
  target_partitions: 40      # Explicit control
```

**Behavior**: Maximum parallelism with partition-key-based distribution.

## How It Works

### 1. Repartitioning Strategy

The script automatically determines the optimal number of partitions:

```python
# Auto-calculate partitions
num_partitions = max(1, row_count // rows_per_partition)

# Repartition by Cassandra partition keys for data locality
df = df.repartition(num_partitions, *partition_keys)
```

**Benefits**:
- Better parallelism across Spark executors
- Data locality with Cassandra partition keys
- Reduced memory pressure per partition
- Improved write throughput

### 2. Caching Strategy

```python
# Cache DataFrame for faster access
df = df.cache()

# ... perform operations ...

# Unpersist after write to free memory
df.unpersist()
```

**Benefits**:
- Faster repeated access to data
- Reduced recomputation
- Better performance for complex transformations

### 3. Coalescing for Small Datasets

```python
if row_count < repartition_threshold:
    optimal_partitions = max(1, min(10, row_count // 10000))
    df = df.coalesce(optimal_partitions)
```

**Benefits**:
- Reduces overhead for small datasets
- Minimizes task scheduling overhead
- Faster processing for small tables

### 4. Batch Write Optimization

The Cassandra connector is configured with:
- **Batch Size**: 1000 rows or 1MB per batch
- **Concurrent Writes**: 5 parallel writes per executor
- **Batch Grouping**: Groups by partition key for efficiency
- **Throughput**: Unlimited for maximum speed

## Performance Monitoring

The script now provides detailed performance metrics:

```
[INFO] Read N rows from dbo.orders in X seconds
[INFO] Optimizing DataFrame with N rows
[INFO] Repartitioning to M partitions for better parallelism
[INFO] Repartitioning by partition keys: ['customer_id']
[INFO] Current partitions after repartition: M
[INFO] Caching DataFrame in memory for faster access
[INFO] Writing data to Cassandra: orders
[INFO] Successfully wrote data to migration_db.orders
[INFO] Unpersisted cached DataFrame
[INFO] Performance: Read=Xs, Write=Ys, Total=Zs
[INFO] Throughput: R rows/sec
```

## Tuning Guidelines

### For Maximum Speed (Large Datasets)

1. **Increase Spark Resources**:
```yaml
spark:
  config:
    spark.driver.memory: "8g"
    spark.executor.memory: "8g"
    spark.executor.cores: 4
```

2. **Optimize Partitions**:
```yaml
optimization:
  rows_per_partition: 50000  # Adjust based on dataset size
  target_partitions: 32      # Or set explicitly
```

3. **Increase Concurrent Writes**:
```yaml
cassandra:
  spark_connector:
    output.concurrent.writes: 10    # More parallel writes
    output.batch.size.rows: 2000    # Larger batches
    output.throughput_mb_per_sec: 1024  # Higher throughput limit
```

### For Memory-Constrained Environments

1. **Reduce Partitions**:
```yaml
optimization:
  rows_per_partition: 100000  # Fewer, larger partitions
```

2. **Disable Caching**:
```yaml
optimization:
  enable_cache: false  # Save memory
```

3. **Reduce Batch Sizes**:
```yaml
cassandra:
  spark_connector:
    output.batch.size.rows: 500
    output.concurrent.writes: 3
```

## Best Practices

1. **Monitor Spark UI**: Check partition distribution and task execution in the Spark web interface

2. **Test with Sample Data**: Start with a subset to find optimal settings

3. **Adjust Based on Row Size**:
   - Large rows (many columns): Reduce `rows_per_partition`
   - Small rows: Increase `rows_per_partition`

4. **Consider Cassandra Cluster Size**:
   - More nodes: Increase `concurrent.writes`
   - Fewer nodes: Keep `concurrent.writes` moderate (3-5)

5. **Use Partition Keys Wisely**: Ensure your Cassandra partition keys provide good distribution

## Troubleshooting

### Issue: Out of Memory Errors
**Solution**: 
- Reduce `rows_per_partition`
- Disable caching
- Increase `spark.executor.memory`

### Issue: Slow Writes to Cassandra
**Solution**:
- Increase `output.concurrent.writes`
- Increase `output.batch.size.rows`
- Check Cassandra cluster health

### Issue: Too Many Small Tasks
**Solution**:
- Increase `rows_per_partition`
- Use `target_partitions` for explicit control
- Enable coalescing for small datasets

### Issue: Uneven Partition Distribution
**Solution**:
- Ensure partition keys have good cardinality
- Consider using composite partition keys
- Check data skew in source tables

## Example Configurations

### Configuration for Medium-Sized Datasets

```yaml
# cassandra_config.yaml
optimization:
  enable_repartition: true
  enable_cache: true
  repartition_threshold: 100000
  rows_per_partition: 50000  # Adjust based on your dataset
  
cassandra:
  spark_connector:
    output.batch.size.rows: 1000
    output.concurrent.writes: 5
    output.batch.grouping.key: partition
```

### Configuration for Very Large Datasets

```yaml
optimization:
  enable_repartition: true
  enable_cache: true
  target_partitions: 50  # Explicit control
  
cassandra:
  spark_connector:
    output.batch.size.rows: 2000
    output.concurrent.writes: 10
    output.throughput_mb_per_sec: 1024
```

## Additional Resources

- [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Cassandra Spark Connector Documentation](https://github.com/datastax/spark-cassandra-connector)
- [Spark Partitioning Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html#partitions)

## Summary

The optimizations provide:
- ✅ Improved resource utilization
- ✅ Automatic optimization based on dataset size
- ✅ Configurable for different scenarios
- ✅ Detailed performance metrics
- ✅ Memory-efficient processing