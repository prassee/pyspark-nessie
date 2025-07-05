.PHONY: help setup start stop logs clean demo query

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Download required JAR files
	@echo "Downloading required JAR files..."
	chmod +x download-jars.sh
	./download-jars.sh

start: ## Start all services
	@echo "Starting all services..."
	docker-compose up -d
	@echo "Services starting... Please wait 2-3 minutes for all services to be ready."
	@echo "You can check status with: make status"

stop: ## Stop all services
	@echo "Stopping all services..."
	docker-compose down

status: ## Check status of all services
	@echo "Service status:"
	docker-compose ps
	@echo ""
	@echo "Service health checks:"
	@echo -n "Nessie: "
	@curl -s -o /dev/null -w "%{http_code}" http://localhost:19120/api/v1/trees || echo "Not ready"
	@echo ""
	@echo -n "Minio: "
	@curl -s -o /dev/null -w "%{http_code}" http://localhost:9000/minio/health/live || echo "Not ready"
	@echo ""

logs: ## Show logs for all services
	docker-compose logs -f

logs-spark: ## Show Spark logs
	docker-compose logs -f spark-master spark-worker

logs-nessie: ## Show Nessie logs
	docker-compose logs -f nessie

logs-minio: ## Show Minio logs
	docker-compose logs -f minio

logs-trino: ## Show Trino logs
	docker-compose logs -f trino

logs-jupyter: ## Show Jupyter logs
	docker-compose logs -f jupyter

demo: ## Run the demo PySpark job
	@echo "Running demo PySpark job..."
	docker exec -it spark-master spark-submit \
		--master spark://spark-master:7077 \
		--jars /opt/spark/jars-custom/*.jar \
		/opt/spark/jobs/iceberg_demo.py

query: ## Open Trino CLI for querying
	@echo "Opening Trino CLI..."
	@echo "Try these commands:"
	@echo "  SHOW CATALOGS;"
	@echo "  SHOW SCHEMAS IN iceberg;"
	@echo "  SHOW TABLES IN iceberg.demo;"
	@echo "  SELECT * FROM iceberg.demo.employees;"
	docker exec -it trino trino

spark-shell: ## Open Spark shell with Iceberg support
	docker exec -it spark-master spark-shell \
		--master spark://spark-master:7077 \
		--jars /opt/spark/jars-custom/*.jar

clean: ## Remove all containers and volumes (WARNING: deletes all data)
	@echo "WARNING: This will delete all data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo ""; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		docker system prune -f; \
	fi

restart: stop start ## Restart all services

urls: ## Show all service URLs
	@echo "Service URLs:"
	@echo "  Spark Master UI:  http://localhost:8080"
	@echo "  Jupyter Lab:      http://localhost:8888"
	@echo "  Minio Console:    http://localhost:9001 (admin/password)"
	@echo "  Minio API:        http://localhost:9000"
	@echo "  Nessie API:       http://localhost:19120"
	@echo "  Trino UI:         http://localhost:8081"

jupyter: ## Open Jupyter Lab
	@echo "Opening Jupyter Lab..."
	@echo "Jupyter Lab will be available at: http://localhost:8888"
	@echo "The notebook 'pyspark_iceberg_demo.ipynb' is ready to run!"
	open http://localhost:8888 2>/dev/null || xdg-open http://localhost:8888 2>/dev/null || echo "Please open http://localhost:8888 in your browser"

fix-permissions: ## Fix notebook file permissions
	@echo "Fixing notebook permissions..."
	sudo chown -R $(USER):$(USER) ./notebooks/
	chmod -R 755 ./notebooks/
	@echo "✅ Permissions fixed!"
