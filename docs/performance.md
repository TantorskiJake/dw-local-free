# Performance and Scale Considerations

This document describes the performance optimizations and scaling strategies implemented in the data warehouse.

## Table Partitioning

### Weather Fact Table Partitioning

The `core.weather` table is partitioned by month using PostgreSQL's native range partitioning:

```sql
CREATE TABLE core.weather (
    ...
) PARTITION BY RANGE (observed_at);
```

**Why Monthly Partitioning?**

1. **Index Size Management**: Monthly partitions keep individual indexes small and manageable
   - Each partition has its own index on `(location_id, observed_at)`
   - Index size grows with partition size, not total table size
   - Old partitions can be dropped or archived without affecting active data

2. **Query Performance**: Time-based queries benefit from partition pruning
   - Queries filtering by date automatically exclude irrelevant partitions
   - PostgreSQL only scans partitions that contain matching data
   - Example: `WHERE observed_at >= '2024-11-01'` only scans November partition

3. **Maintenance Efficiency**: 
   - VACUUM and ANALYZE operations run faster on smaller partitions
   - Index maintenance is isolated per partition
   - Can drop old partitions without rebuilding entire table

4. **Concurrent Access**:
   - Different partitions can be written to concurrently
   - Reduces lock contention during inserts
   - Better parallel query execution

**Index Strategy**

The index on `(location_id, observed_at)` is created on the partitioned table:

```sql
CREATE INDEX idx_weather_location_observed 
    ON core.weather (location_id, observed_at);
```

This creates a **local index** on each partition automatically. Benefits:
- Index size per partition is small (proportional to partition size)
- Queries filtering by location and time use partition pruning + index scan
- Index maintenance is faster (per partition, not entire table)

**Partition Management**

New monthly partitions should be created proactively:

```sql
-- Create partition for December 2024
CREATE TABLE core.weather_2024_12 PARTITION OF core.weather
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');
```

Old partitions can be archived or dropped:

```sql
-- Archive old partition (move to archive schema)
ALTER TABLE core.weather DETACH PARTITION core.weather_2024_01;

-- Or drop if no longer needed
DROP TABLE core.weather_2024_01;
```

## Concurrency Control

### API Rate Limiting

The pipeline processes 10 locations but limits concurrent API calls to 3:

```python
# Process 10 locations with max 3 concurrent HTTP calls
with concurrency("weather_api_calls", occupy=1, limit=3):
    weather_fetch_results = fetch_raw_weather.map(locations)
```

**Benefits:**
- Prevents overwhelming external APIs (Open-Meteo, Wikipedia)
- Respects API rate limits
- Reduces risk of throttling or blocking
- Still processes efficiently (3 at a time vs sequential)

**Execution Pattern:**
- 10 locations processed in batches of 3
- First batch: Locations 1-3 execute concurrently
- Second batch: Locations 4-6 execute concurrently
- Third batch: Locations 7-9 execute concurrently
- Final batch: Location 10 executes

Total time: ~4 batches × batch_time (vs 10 × single_time if sequential)

## Materialized View Refresh

### Concurrent Refresh

Materialized views are refreshed using the `CONCURRENTLY` option:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY mart.daily_weather_aggregates;
```

**Why CONCURRENTLY?**

1. **Non-Blocking Reads**: 
   - Queries can continue reading the old version during refresh
   - No downtime or query blocking
   - Critical for production systems with continuous queries

2. **Availability**:
   - Analytics dashboards remain accessible during refresh
   - No "table locked" errors for users
   - Better user experience

**Requirements:**

- Unique index must exist on the materialized view (already created):
  ```sql
  CREATE UNIQUE INDEX idx_daily_weather_agg_unique 
      ON mart.daily_weather_aggregates (location_name, observation_date);
  ```

- Refresh takes longer (builds new version before swapping)
- More disk space temporarily (old + new versions during refresh)

**Refresh Strategy:**

Views are refreshed concurrently after data quality checks pass:

```python
refresh_results = refresh_materialized_view.map(view_names)
```

Both views refresh in parallel (concurrent execution), and each uses CONCURRENTLY internally.

## Scaling with Citus (Horizontal Sharding)

### When to Consider Sharding

As data volume grows, consider horizontal sharding using [Citus](https://www.citusdata.com/) (open-source PostgreSQL extension):

**Indicators:**
- Weather fact table exceeds 100GB per partition
- Query performance degrades despite partitioning
- Need to scale beyond single PostgreSQL instance
- Multiple locations with high data volume

### Citus Sharding Strategy

**Shard by `location_id`:**

```sql
-- Enable Citus extension
CREATE EXTENSION IF NOT EXISTS citus;

-- Convert weather table to distributed table
SELECT create_distributed_table(
    'core.weather',
    'location_id',
    colocate_with => 'none'
);
```

**Why Shard by Location?**

1. **Data Locality**: 
   - All weather data for a location stays on same shard
   - Queries filtering by location hit single shard
   - Joins with location dimension are efficient

2. **Parallel Processing**:
   - Each shard processes independently
   - Queries can execute in parallel across shards
   - Better utilization of multiple nodes

3. **Scaling Pattern**:
   - Add more shards as locations increase
   - Each shard handles subset of locations
   - Linear scaling with number of nodes

**Implementation (Local, No Cloud Cost):**

Citus can run locally using Docker Compose:

```yaml
services:
  citus-coordinator:
    image: citusdata/citus:latest
    environment:
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
  
  citus-worker-1:
    image: citusdata/citus:latest
    environment:
      POSTGRES_PASSWORD: postgres
    depends_on:
      - citus-coordinator
  
  citus-worker-2:
    image: citusdata/citus:latest
    environment:
      POSTGRES_PASSWORD: postgres
    depends_on:
      - citus-coordinator
```

**Benefits:**
- No cloud costs (runs on local hardware)
- Horizontal scaling by adding worker nodes
- Transparent to application (still uses PostgreSQL)
- Can scale to 100+ nodes if needed

**Considerations:**
- Cross-shard queries have overhead
- Transactions spanning shards are more complex
- Requires Citus extension (open-source, free)
- Best for read-heavy, location-partitioned workloads

### Migration Path

1. **Start with partitioning** (current approach) - handles moderate scale
2. **Monitor growth** - track partition sizes and query performance
3. **Add Citus when needed** - migrate to distributed when single node limits reached
4. **Shard by location_id** - natural partitioning for weather data

## Performance Monitoring

### Key Metrics to Track

1. **Partition Sizes**: Monitor monthly partition growth
   ```sql
   SELECT 
       schemaname,
       tablename,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
   FROM pg_tables
   WHERE tablename LIKE 'weather_%'
   ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
   ```

2. **Index Sizes**: Track index growth per partition
   ```sql
   SELECT 
       indexname,
       pg_size_pretty(pg_relation_size(indexname::regclass)) as index_size
   FROM pg_indexes
   WHERE tablename LIKE 'weather_%';
   ```

3. **Query Performance**: Monitor slow queries
   ```sql
   -- Enable query logging for queries > 1 second
   SET log_min_duration_statement = 1000;
   ```

4. **Concurrency Metrics**: Track API call patterns in Prefect UI

## Best Practices

1. **Proactive Partition Creation**: Create next month's partition before it's needed
2. **Regular Maintenance**: VACUUM and ANALYZE partitions regularly
3. **Archive Old Data**: Move old partitions to archive schema or cold storage
4. **Monitor Growth**: Set alerts for partition size thresholds
5. **Test Sharding**: Validate Citus setup before production migration

## Summary

- **Current**: Monthly partitioning + local indexes for moderate scale
- **Future**: Citus sharding by location_id for horizontal scaling
- **Concurrency**: Rate-limited API calls (3 concurrent max)
- **Views**: Concurrent refresh for zero-downtime updates
- **Cost**: All solutions work locally, no cloud spend required

