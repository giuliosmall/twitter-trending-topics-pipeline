# Twitter Data Pipeline with Spark and MinIO

## Project Overview

This project demonstrates how to build a data pipeline for trending topic detection using Apache Spark and MinIO. It uses PySpark for distributed data processing and MinIO as the object storage solution. The pipeline reads Twitter JSON data, processes it, and extracts trending topics using various natural language processing techniques including tokenization, stopword removal, named entity recognition, n-gram generation, and TF-IDF scoring.

The project is containerized using Docker and can run seamlessly on both ARM and x86 architectures.

## Folder Structure

```
twitter-trending-topics-pipeline/
├── app/
│   ├── main.py                  # Entry point — orchestrates the full pipeline
│   ├── config.py                # Environment config, Spark session, spaCy model loading
│   ├── preprocessing.py         # Text cleaning, stopword removal, hashtag extraction, NER
│   ├── processing.py            # Data loading, NLP pipeline, TF-IDF, windowing, ranking
│   ├── io_utils.py              # Read/write helpers for local and MinIO storage
│   ├── dashboard.py             # Streamlit word cloud dashboard
│   ├── test_main.py             # Unit and integration tests
│   ├── requirments.txt          # Python dependencies (used by Dockerfile)
│   └── __init__.py              # Package init
├── data/
│   ├── dataset.json             # Full sample Twitter dataset
│   └── test_sample.json         # Small sample for tests
├── Dockerfile                   # Spark image with all dependencies
├── docker-compose.yml           # Spark, MinIO, and Streamlit services
├── Makefile                     # Common Docker and dev commands
├── pyproject.toml               # Ruff linting/formatting and pytest config
├── architecture.py              # AWS architecture diagram generator
├── cloudformation.yaml          # CloudFormation template (future AWS deployment)
├── future_scope.md              # Scalable AWS architecture roadmap
└── README.md
```

## Requirements

1. **Docker** and **Docker Compose** installed on your local machine.
2. **Python 3.10+** (if you wish to run the code outside the container).
3. **Make** (optional but recommended). Install with `brew install make` on macOS or `sudo apt install make` on Ubuntu.

## Setup and Configuration

### Step 1: Build and Run the Docker Containers

1. **Build the Docker image:**

    ```bash
    make build
    ```

   This may take several minutes depending on your internet speed and Docker resources.

2. **Start the Docker containers:**

    ```bash
    make up
    ```

3. **Access MinIO:**

   Once the containers are up, MinIO will be available at [http://localhost:9001](http://localhost:9001) with the following credentials:

   - **Username**: `minioadmin`
   - **Password**: `minioadmin`

4. **Create a bucket in MinIO:**

   Go to the MinIO UI, create a new bucket named `tweets-bucket`, and upload `dataset.json` and `test_sample.json` from the `data/` folder.

### Step 2: Run the Spark Application

1. **Connect to the Spark container:**

    ```bash
    make shell
    ```

2. **Run the pipeline:**

    ```bash
    python3 main.py
    ```

3. **Output:**

   The processed results will be saved as CSV in the `output/` directory (or in the MinIO bucket when `USE_MINIO=true`).

### Step 3: Run Tests

While connected to the Spark container:

```bash
pytest test_main.py
```

Or from the host via Make:

```bash
make test
```

### Step 4: Linting and Formatting

Install [ruff](https://docs.astral.sh/ruff/) locally (`pip install ruff`), then:

```bash
make lint      # Check for lint issues
make format    # Auto-format code
```

Configuration is in `pyproject.toml`.

### Step 5: Accessing the Streamlit Dashboard

The Streamlit dashboard generates a word cloud from the pipeline output.

After running the Spark job, navigate to [http://localhost:8501](http://localhost:8501).

### Live Code Editing

The `app/` directory is mounted from the host into the container, so local file changes are reflected immediately without rebuilding the image.

## Makefile Reference

| Command        | Description                                |
|----------------|--------------------------------------------|
| `make build`   | Build the Docker image                     |
| `make up`      | Start all containers                       |
| `make down`    | Stop all containers                        |
| `make shell`   | Open a shell in the Spark container        |
| `make run-spark` | Run the Spark job via `spark-submit`     |
| `make run-python` | Run the Spark job via `python3`         |
| `make test`    | Run pytest in the Spark container          |
| `make lint`    | Lint code with ruff                        |
| `make format`  | Format code with ruff                      |
| `make clean`   | Remove `output/` directory                 |
| `make rebuild` | Full clean rebuild (down + clean + build + up) |

## Ports

| Service    | Container Port | Host Port |
|------------|---------------|-----------|
| Spark UI   | 4040          | 4040      |
| Spark Master | 8080        | 8180      |
| Spark Worker | 7077        | 7077      |
| Spark REST | 6066          | 6066      |
| MinIO API  | 9000          | 9000      |
| MinIO Console | 9001       | 9001      |
| Streamlit  | 8501          | 8501      |

## Troubleshooting

1. **Port conflicts:**
   If a port is already in use, edit the host port mapping in `docker-compose.yml` (e.g., `"8180:8080"` maps host 8180 to container 8080).

2. **Spark hostname resolution failure:**
   The `SPARK_LOCAL_HOSTNAME=localhost` environment variable in `docker-compose.yml` prevents Spark from failing to resolve the container ID as a hostname.

3. **Permission issues:**
   If you encounter permission errors during the Docker build, ensure that the Dockerfile is using `USER root` where necessary.

4. **MinIO not accessible:**
   Ensure the MinIO container is running (`docker ps`) and accessible on port 9000.

5. **Hadoop/Spark version compatibility:**
   If you see class-not-found errors, verify the Hadoop and Spark JAR versions are compatible.

6. **Docker Compose build fails:**
   Try running with `sudo` or check file permissions in the project directory.
