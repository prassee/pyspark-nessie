# PySpark + Nessie + Iceberg + Trino Data Lake Setup

This project sets up a complete data lake architecture using Docker Compose with the following components:

- **Spark Master & Worker**: For running PySpark jobs
- **Minio**: S3-compatible object storage
- **Nessie**: Git-like catalog for data versioning
- **Trino**: Distributed SQL query engine
- **Iceberg**: Table format for analytics

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   PySpark   │────│   Nessie    │────│    Minio    │
│   Jobs      │    │  Catalog    │    │   Storage   │
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌─────────────┐
                    │    Trino    │
                    │   Query     │
                    └─────────────┘
```

## Prerequisites

- Docker and Docker Compose
- wget (for downloading JAR files)
- curl (for health checks)

## Quick Start

1. **Download required JAR files**:
   ```bash
   chmod +x download-jars.sh
   ./download-jars.sh
   ```

2. **Start all services**:
   ```bash
   docker-compose up -d
   ```

3. **Wait for services to be ready** (about 2-3 minutes):
   ```bash
   # Check service status
   docker-compose ps
   
   # Check logs if needed
   docker-compose logs -f nessie
   docker-compose logs -f minio
   ```

4. **Run the sample PySpark job**:
   ```bash
   docker exec -it spark-master spark-submit \
     --master spark://spark-master:7077 \
     --jars /opt/spark/jars-custom/*.jar \
     /opt/spark/jobs/iceberg_demo.py
   ```

5. **Or use Jupyter Notebook** (recommended for interactive exploration):
   ```bash
   make jupyter
   # Open the pyspark_iceberg_demo.ipynb notebook
   ```

6. **Or run the Scala equivalent**:
   ```bash
   # Build Scala project
   ./build-scala.sh
   
   # Run Scala demo
   make scala-demo
   ```

## Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Spark Master UI | http://localhost:8080 | - |
| Jupyter Lab | http://localhost:8888 | - |
| Minio Console | http://localhost:9001 | admin/password |
| Minio API | http://localhost:9000 | admin/password |
| Nessie API | http://localhost:19120 | - |
| Trino UI | http://localhost:8081 | - |

## Using Trino to Query Iceberg Tables

1. **Connect to Trino CLI**:
   ```bash
   docker exec -it trino trino
   ```

2. **Query your Iceberg tables**:
   ```sql
   SHOW CATALOGS;
   SHOW SCHEMAS IN iceberg;
   SHOW TABLES IN iceberg.demo;
   SELECT * FROM iceberg.demo.employees;
   ```

## Directory Structure

```
.
├── docker-compose.yml          # Main Docker Compose configuration
├── conf/
│   └── spark-defaults.conf     # Spark configuration
├── jars/                       # Downloaded JAR files
├── src/
│   └── iceberg_demo.py         # Sample PySpark job
├── scala-spark/                # Scala equivalent project
│   ├── build.sbt              # SBT build configuration
│   ├── src/main/scala/        # Scala source code
│   └── src/test/scala/        # Scala test code
├── trino/
│   └── etc/                    # Trino configuration files
└── download-jars.sh            # Script to download dependencies
```

## Sample PySpark Job

The included `src/iceberg_demo.py` demonstrates:

- Creating a namespace in Nessie catalog
- Writing data to Iceberg tables
- Reading data back
- Appending data to existing tables
- Viewing table history

## Configuration Details

### Spark Configuration
- Iceberg Spark extensions enabled
- Nessie catalog configured as default
- S3A filesystem configured for Minio
- Proper serialization settings

### Nessie Configuration
- In-memory version store (for demo)
- REST API exposed on port 19120
- Git-like branching and tagging support

### Minio Configuration
- S3-compatible API on port 9000
- Web console on port 9001
- Automatic bucket creation (`warehouse`, `nessie`)

### Trino Configuration
- Iceberg connector with Nessie catalog
- Native S3 filesystem support
- Production-ready settings

## Advanced Usage

### Creating Branches in Nessie

```python
# In your PySpark job
spark.sql("CREATE BRANCH feature_branch IN nessie FROM main")
spark.sql("USE nessie.feature_branch")
# ... make changes ...
spark.sql("MERGE BRANCH feature_branch INTO main IN nessie")
```

### Schema Evolution

```python
# Add columns to existing table
spark.sql("ALTER TABLE nessie.demo.employees ADD COLUMN email STRING")
```

### Time Travel Queries

```sql
-- In Trino
SELECT * FROM iceberg.demo.employees FOR VERSION AS OF 'snapshot_id_here';
```

## Troubleshooting

### Common Issues

1. **Services not starting**: Check if ports are available
2. **JAR files missing**: Run `./download-jars.sh`
3. **Connection errors**: Wait for all services to be healthy

### Checking Service Health

```bash
# Check all container status
docker-compose ps

# Check specific service logs
docker-compose logs nessie
docker-compose logs minio
docker-compose logs spark-master

# Test Nessie API
curl http://localhost:19120/api/v1/trees

# Test Minio
curl http://localhost:9000/minio/health/live
```

## Scaling

To add more Spark workers:

```bash
docker-compose up -d --scale spark-worker=3
```

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v
```

## Next Steps

1. Explore Nessie's branching and merging capabilities
2. Set up CI/CD pipelines for your data pipelines
3. Implement data quality checks
4. Configure monitoring and alerting
5. Set up production-grade storage and security
