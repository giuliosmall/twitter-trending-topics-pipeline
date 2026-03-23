# Twitter Trending Topics Pipeline

## Project Overview

A data pipeline that detects **trending topics** from Twitter data using Apache Spark and MinIO. Unlike simple word-frequency approaches, this pipeline uses **burst detection** — ranking topics by how much their usage spikes in a given time window relative to their baseline, similar to how Twitter's real trending algorithm works.

The NLP pipeline uses spaCy for **POS-based filtering** (keeping only nouns, proper nouns, and hashtags), **word-frequency filtering** (removing generic high-frequency nouns), and **bigram generation** to surface meaningful phrases. Results are displayed per time window with burst scores.

The project is containerized using Docker and runs on both ARM and x86 architectures.

### Example Output

```
+-------------------+-------------------+----------+-----+-----------+----+
|start_time         |end_time           |topic     |count|burst_score|rank|
+-------------------+-------------------+----------+-----+-----------+----+
|2012-02-04 09:00:00|2012-02-04 10:00:00|aflevering|5    |2.5        |1   |
|2012-02-04 09:00:00|2012-02-04 10:00:00|verjaardag|6    |1.88       |2   |
|2012-02-04 10:00:00|2012-02-04 11:00:00|kamer     |17   |2.76       |1   |
|2012-02-04 10:00:00|2012-02-04 11:00:00|bed       |30   |2.73       |2   |
|2012-02-04 11:00:00|2012-02-04 12:00:00|opendag   |6    |3.0        |1   |
|2012-02-04 11:00:00|2012-02-04 12:00:00|amsterdam |8    |2.82       |4   |
+-------------------+-------------------+----------+-----+-----------+----+
```

A `burst_score` of 3.0 means the term was mentioned 3x more than its average across all windows.

## How It Works

1. **Text cleaning** — Strips URLs, HTML entities, mentions, special characters. Preserves hashtags.
2. **POS-based token extraction** — spaCy tags each token; only `NOUN`, `PROPN`, and hashtags are kept. Verbs, adjectives, adverbs, pronouns are automatically filtered in any language.
3. **Frequency filtering** — Nouns ranked among the top 700 most common words in the spaCy vocabulary are dropped (e.g. "people", "time", "uur"). Prevents generic words from dominating results.
4. **Noise filtering** — Removes HTML entity remnants (`lt3`, `amp`), internet slang (`lol`, `rt`), and laughter variants (`hahaha`).
5. **Bigram generation** — Produces two-word phrases from filtered tokens.
6. **Burst detection** — For each term in each time window: `burst_score = count / avg_count_across_all_windows`. Terms are ranked by burst score, not raw frequency.
7. **Top-N selection** — Returns the top N trending topics per window, with a minimum count floor to filter noise.

## Folder Structure

```
twitter-trending-topics-pipeline/
├── app/
│   ├── main.py                  # Entry point — orchestrates the full pipeline
│   ├── config.py                # Environment config, Spark session, spaCy model loading
│   ├── preprocessing.py         # Text cleaning, POS filtering, noise/frequency filtering
│   ├── processing.py            # Data loading, term extraction, burst detection, ranking
│   ├── io_utils.py              # Read/write helpers for local and MinIO storage
│   ├── dashboard.py             # Streamlit word cloud dashboard
│   ├── test_main.py             # Unit and integration tests
│   ├── requirments.txt          # Python dependencies (used by Dockerfile)
│   └── __init__.py              # Package init
├── data/
│   ├── dataset.json             # Full sample Twitter dataset (~9K tweets, Feb 4 2012)
│   └── test_sample.json         # Small sample for tests (3 tweets)
├── Dockerfile                   # Spark image with optimized layer caching
├── docker-compose.yml           # Spark, MinIO, and Streamlit services
├── Makefile                     # Common Docker and dev commands
├── pyproject.toml               # Ruff linting/formatting and pytest config
├── .dockerignore                # Excludes .git, __pycache__, docs from build context
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

   First build takes several minutes (installs apt packages, pip dependencies, spaCy `md` models, Hadoop JARs). Subsequent builds are fast due to layer caching — code changes only rebuild the final `COPY` layer.

2. **Start the Docker containers:**

    ```bash
    make up
    ```

3. **Access MinIO:**

   Once the containers are up, MinIO will be available at [http://localhost:9001](http://localhost:9001) with the following credentials:

   - **Username**: `minioadmin`
   - **Password**: `minioadmin`

4. **Create a bucket in MinIO:**

   Go to the MinIO UI, create a new bucket named `tweets-bucket`, and upload `dataset.json` and `test_sample.json` from the `data/` folder into a `data/` prefix.

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

   The pipeline prints trending topics per hourly window with burst scores, and saves results as CSV in the MinIO bucket (or local `output/` directory).

### Step 3: Run Tests

While connected to the Spark container:

```bash
pytest test_main.py
```

Or from the host via Make:

```bash
make test
```

Test coverage includes:
- Text cleaning (URLs, HTML entities, mentions, hashtags, edge cases)
- Stopword and noise token filtering
- POS-based topical token extraction
- Word frequency filtering
- NER (named entity recognition)
- Full pipeline integration (burst detection, windowing, ranking)
- I/O (local read/write, MinIO paths)
- Empty DataFrame edge cases
- Config and input validation

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

The dashboard includes error handling for MinIO connection failures, missing data, and empty results.

### Live Code Editing

The `app/` directory is mounted from the host into the container, so local file changes are reflected immediately without rebuilding the image. Only rebuild (`make rebuild`) when dependencies change.

## Makefile Reference

| Command          | Description                                  |
|------------------|----------------------------------------------|
| `make build`     | Build the Docker image                       |
| `make up`        | Start all containers                         |
| `make down`      | Stop all containers                          |
| `make shell`     | Open a shell in the Spark container          |
| `make run-spark` | Run the Spark job via `spark-submit`         |
| `make run-python`| Run the Spark job via `python3`              |
| `make test`      | Run pytest in the Spark container            |
| `make lint`      | Lint code with ruff                          |
| `make format`    | Format code with ruff                        |
| `make clean`     | Remove `output/` directory                   |
| `make rebuild`   | Full clean rebuild (down + clean + build + up)|

## Ports

| Service        | Container Port | Host Port |
|----------------|---------------|-----------|
| Spark UI       | 4040          | 4040      |
| Spark Master   | 8080          | 8180      |
| Spark Worker   | 7077          | 7077      |
| Spark REST     | 6066          | 6066      |
| MinIO API      | 9000          | 9000      |
| MinIO Console  | 9001          | 9001      |
| Streamlit      | 8501          | 8501      |

## Key Configuration

| Parameter        | Default | Description |
|------------------|---------|-------------|
| `N`              | 5       | Number of trending topics per time window |
| `time_interval`  | hour    | Window size: "hour", "day", or "week" |
| `min_count`      | 2       | Minimum mentions for a term to be considered |
| `min_freq_rank`  | 700     | spaCy frequency rank threshold (filters common nouns) |

These can be adjusted in `main.py` or passed as parameters to `run_pipeline()`.

## Troubleshooting

1. **Port conflicts:**
   If a port is already in use, edit the host port mapping in `docker-compose.yml` (e.g., `"8180:8080"` maps host 8180 to container 8080).

2. **Spark hostname resolution failure:**
   The `SPARK_LOCAL_HOSTNAME=localhost` environment variable in `docker-compose.yml` prevents Spark from failing to resolve the container ID as a hostname.

3. **spaCy model errors:**
   The pipeline uses `md` (medium) models for word frequency data. If you see `ConfigSchemaNlp` errors, ensure spaCy is pinned to `>=3.7,<3.8` in the Dockerfile.

4. **Spark worker OOM / Python worker crashed:**
   spaCy models are loaded lazily per worker process (not via UDF closure). If workers still crash, increase Docker memory limits.

5. **MinIO not accessible:**
   Ensure the MinIO container is running (`docker ps`) and accessible on port 9000. The Spark container connects via `MINIO_SERVER=http://minio:9000`.

6. **Stale bytecode after code changes:**
   If changes don't take effect, clear `__pycache__`: `rm -rf /home/sparkuser/app/__pycache__` inside the container.

7. **Docker Compose build fails:**
   Try running with `sudo` or check file permissions in the project directory.
