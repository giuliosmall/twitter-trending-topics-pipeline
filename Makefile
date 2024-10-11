# Variables
DOCKER_COMPOSE = docker compose
DOCKER_CONTAINER = spark-container
SPARK_APP_DIR = /home/sparkuser/app

# Build the Docker image
build:
	$(DOCKER_COMPOSE) build

# Start the Docker containers (Spark and MinIO)
up:
	$(DOCKER_COMPOSE) up -d

# Stop the Docker containers
down:
	$(DOCKER_COMPOSE) down

# Access the Spark container via bash
shell:
	docker exec -it $(DOCKER_CONTAINER) /bin/bash

# Run the Spark job (in Docker)
run-spark:
	docker exec -it $(DOCKER_CONTAINER) bash -c "cd $(SPARK_APP_DIR) && spark-submit --master local[*] main.py"

# Run the Spark job using Python
run-python:
	docker exec -it $(DOCKER_CONTAINER) bash -c "cd $(SPARK_APP_DIR) && python3 main.py"

# Run the unit tests
test:
	docker exec -it $(DOCKER_CONTAINER) bash -c "cd $(SPARK_APP_DIR) && pytest test_main.py"

# Clean up output files
clean:
	rm -rf output/

# Rebuild everything (clean, build, and start)
rebuild: down clean build up