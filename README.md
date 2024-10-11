# Twitter Data Pipeline with Spark and MinIO

## Project Overview

This project demonstrates how to build a data pipeline for trending topic detection using Apache Spark and MinIO. It uses PySpark for distributed data processing and MinIO as the object storage solution. The pipeline reads Twitter JSON data, processes it, and extracts trending topics using various natural language processing techniques.

The project is containerized using Docker and can run seamlessly on both ARM and x86 architectures.

## Folder Structure

```
twitter-data-pipeline/
├── app/
│   ├── main.py                  # Main Spark application script
│   ├── test_main.py             # Unit tests for the main application
│   ├── requirements.txt         # Python dependencies for the project (optional)
│   └── __init__.py              # (Optional) To make the directory a package
├── data/
│   ├── dataset.json             # Sample input file for main.py (used locally)
│   ├── test_sample.json         # Sample input file for test_main.py (used locally)
├── Dockerfile                   # Dockerfile to build Spark image with dependencies
├── docker-compose.yml           # Docker Compose file to set up Spark and MinIO services
├── Makefile                     # Makefile for common Docker commands
├── architecture.py              # Architecture diagram if the project was deployed on AWS 
├── cloudformation.yaml          # CloudFormation template for deploying the architecture (IaC)
├── README.md                    # Documentation for the project
├── future_scope.md              # Architecture for a scalable, production-ready AWS system
```

## Requirements

1. **Docker** and **Docker Compose** installed on your local machine.
2. **Python 3.7+** (if you wish to run the code outside the container).
3. **Make** (optional but recommended for running the commands in the `Makefile`). You can install it by running `sudo apt install make` on Ubuntu or `brew install make` on macOS.

## Setup and Configuration

### Step 1: Build and Run the Docker Containers

1. **Build the Docker image:**

    Navigate to the root directory (`twitter-data-pipeline/`) and run the following command to build the custom Spark image:

    ```bash
    make build
    ```
   
   Depending on your internet speed and the amount of hardware allocated to Docker, this process may take up to 8 minutes.

2. **Start the Docker containers:**

    Run the following command to start both the Spark and MinIO containers:

    ```bash
    make up
    ```

3. **Access MinIO:**

   Once the containers are up, MinIO will be available at [http://localhost:9001](http://localhost:9001) with the following credentials:

   - **Username**: `minioadmin`
   - **Password**: `minioadmin`

4. **Create a bucket in MinIO:**

   Go to the MinIO UI, create a new bucket named `tweets-bucket`, and upload the sample `dataset.json` and `test_sample.json` file located in the `data/` folder.

### Step 2: Run the Spark Application

1. **Connect to the Spark container:**

    To run the application, first connect to the running Spark container:

    ```bash
    make shell
    ```

2. **Run the Spark job:**

    Inside the container, navigate to the `app` folder and run the `main.py` script:

   Run the Spark Job in docker: 
   ```bash
    python3 main.py
    ```

3. **Output:**

   The processed results will be saved in the `output/` folder in your project directory.

### Step 3: Run Tests

1. **Run Unit Tests:**

   While connected to the Spark container, navigate to the `app` folder and run the `test_main.py` script using `pytest`:

   ```bash
   pytest test_main.py
   ```
    The tests should execute successfully, confirming that the pipeline is working correctly.

### Step 4: Accessing and Modifying Files

The `app/` directory is mounted from the host to the container, so any changes you make to the Python files locally will be reflected in the container. 
This eliminates the need to rebuild the Docker image for every code change.

Important Configuration Details

- **MinIO Configuration:**
The `MINIO_SERVER`, `MINIO_ACCESS_KEY`, and `MINIO_SECRET_KEY` environment variables are configured in the `docker-compose.yml` file. These variables allow Spark to connect to the MinIO server.

- **Spark Configuration:**
In `main.py`, additional Spark configurations are added to support S3 file access

### Step 5: Accessing the Streamlit dashboard
The Streamlit dashboard is a very simple interface designed to showcase the output of the Spark job in a more visual and interactive manner. 
Specifically, it generates a word cloud from the trending Twitter topics.

To access the dashboard, navigate to http://localhost:8501 after running the Docker container.


## Troubleshooting

1.	Permission Issues:
	•	If you encounter permission errors during the Docker build, ensure that the Dockerfile is using USER root where necessary.
    
2.	MinIO Not Accessible:
	•	Ensure the MinIO container is running and accessible on port 9000.

3.	Hadoop/Spark Version Compatibility:
	•	If you see errors related to class not found or version mismatches, verify the Hadoop and Spark versions are compatible.

4.	Docker Compose Build Fails:
	•	If the build fails due to missing dependencies or permission issues, try running the build command with sudo or check the permissions of your files.