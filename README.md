# PySpark + Nessie + Iceberg + Trino Data Lake Setup

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

7. **Run sync job every 7 minutes**:
   ```bash
   # For one-time manual sync
   podman-compose down olake-sync ; podman-compose up -d olake-sync --build
   
   # For automated 7-minute intervals (using cron)
   */7 * * * * cd /path/to/project && podman-compose down olake-sync && podman-compose up -d olake-sync --build
   ```


make  scala-demo ARGS="olakeCdc -t order_items -d 2025/07/23"
make  scala-demo ARGS="olakeCdc -t order_items -d 2025/07/24"
make  scala-demo ARGS="olakeCdc -t order_items -d 2025/07/25"
make  scala-demo ARGS="olakeCdc -t order_items -d 2025/07/26"
make  scala-demo ARGS="olakeCdc -t orders -d 2025/07/23"
make  scala-demo ARGS="olakeCdc -t orders -d 2025/07/24"
make  scala-demo ARGS="olakeCdc -t orders -d 2025/07/25"
make  scala-demo ARGS="olakeCdc -t orders -d 2025/07/26"
make  scala-demo ARGS="olakeCdc -t customers -d 2025/07/23"
make  scala-demo ARGS="olakeCdc -t customers -d 2025/07/24"
make  scala-demo ARGS="olakeCdc -t customers -d 2025/07/25"
make  scala-demo ARGS="olakeCdc -t customers -d 2025/07/26"

make  scala-demo ARGS="olakeCdc -t order_items -d 2025/07/25"
make  scala-demo ARGS="olakeCdc -t orders -d 2025/07/25"
make  scala-demo ARGS="olakeCdc -t customers -d 2025/07/24"

make  scala-demo ARGS="olakeCdc -t customers -d 2025/07/25 -f"
make  scala-demo ARGS="olakeCdc -t customers -d 2025/07/26" 
make  scala-demo ARGS="olakeCdc -t orders -d 2025/07/26 -f" 
make  scala-demo ARGS="olakeCdc -t order_items -d 2025/07/26 -f"


make  scala-demo ARGS="olakeCdc -t orders -d 2025/07/27" 
make  scala-demo ARGS="olakeCdc -t order_items -d 2025/07/26"
make  scala-demo ARGS="olakeCdc -t order_items -d 2025/07/27"