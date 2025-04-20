# PyFlink Examples (Updated)

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg?style=flat)](http://www.apache.org/licenses/LICENSE-2.0.html)

A collection of examples demonstrating Apache Flink™'s Python API (PyFlink), updated to use modern APIs and run within a self-contained Docker environment.

These examples primarily use the PyFlink **Table API**, showcasing common patterns for batch processing.

## Overview of the Docker Approach

Running PyFlink applications typically requires a Java runtime environment, Python, and specific dependencies (like PyFlink itself) to be available to the Flink cluster nodes. To simplify setup and ensure consistency, this project uses **Docker Compose** to manage a local Flink cluster built from a custom **Dockerfile**.

Here's the workflow:
1.  You use `docker compose up --build` (implicitly handled by `make start-flink` if the image doesn't exist) to build a custom Flink Docker image based on the included `Dockerfile`. This image installs Python 3 and the necessary Python packages (`apache-flink`, `numpy`) on top of the official Flink image.
2.  Docker Compose then starts Flink JobManager and TaskManager containers using this custom image.
3.  The project directory is **mounted** as a volume inside these containers, making your Python scripts accessible to Flink.
4.  You use `make run` which executes `flink run` commands *inside* the JobManager container.
5.  The `flink run` command submits your Python scripts (`.py` files) to the Flink cluster.
6.  Flink executes the Python scripts using the Python 3 environment built into the Docker image, potentially distributing tasks to the TaskManager(s).
7.  Any output printed by the Python scripts (like results) appears in the standard output logs of the **Flink TaskManager** containers.

This approach ensures the correct Java, Python, and Python dependencies are present and avoids configuration issues related to finding the Python executable.

## Requirements

*   [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
*   Python 3.6+ (Optional, for local utilities if desired)
*   [uv](https://github.com/astral-sh/uv) (Optional, for local setup)
*   [Homebrew](https://brew.sh/) (Optional, on macOS, used by the Makefile to install `uv` if not found)

## Dockerfile and `docker-compose.yml` Setup

*   **`Dockerfile`:**
    *   Starts from the official `flink:1.19.0-scala_2.12-java11` image.
    *   Installs `python3` and `python3-pip` using `apt-get`.
    *   Copies `requirements.txt`.
    *   Installs Python dependencies (`apache-flink`, `numpy`) using `pip3`.
*   **`docker-compose.yml`:**
    *   Defines `jobmanager` and `taskmanager` services.
    *   Uses `build: .` to instruct Docker Compose to build the image using the `Dockerfile` in the current directory.
    *   Exposes port `8081` for the Flink Web UI.
    *   Sets basic Flink configuration.
    *   Mounts the current project directory (`.`) to `/opt/flink/usrlib` inside the containers.
    *   Connects the services via a `flink-network`.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/wdm0006/flink-python-examples.git
    cd flink-python-examples
    ```

2.  **(Optional) Set up local Python environment:**
    This step is optional for just running the examples via Docker.
    ```bash
    make setup
    ```
    This uses `uv` to create a `.venv` and install Python dependencies locally.

3.  **Build the Image and Start the Flink Cluster:**
    The first time you run this, Docker Compose will build the image defined in the `Dockerfile`. Subsequent runs will reuse the existing image unless the `Dockerfile` or its context changes.
    ```bash
    make start-flink
    # Or directly: docker compose up -d --build
    ```
    You can access the Flink Web UI at [http://localhost:8081](http://localhost:8081).

## Running the Examples

With the Flink cluster running, submit the example jobs:

```bash
make run
```

This command uses `docker compose exec jobmanager flink run -py <script_path_in_container>` for each example script. Flink uses the Python 3 environment built into the Docker image.

**Output:** Check the **Flink Web UI** ([http://localhost:8081](http://localhost:8081)) for running/completed jobs. View the `stdout` logs of the TaskManager(s) to see printed results.

To submit a single example (e.g., word count):
```bash
make submit_word_count
```

## Stopping the Cluster

```bash
make stop-flink
# Or directly: docker compose down
```

## Examples Included

*   **Word Count:** Counts word occurrences in a predefined string.
*   **Trending Hashtags:** Generates sample "tweets", extracts hashtags, and counts their frequency.
*   **Data Enrichment:** Reads sample JSON data and a CSV dimension table, joins them based on an attribute, and outputs the enriched data.
*   **Mean Values:** Generates sample floating-point data and calculates the mean of each column.
*   **Mandelbrot Set:** Generates candidate complex numbers and identifies points within the Mandelbrot set.
*   **Template Example:** A basic skeleton (`template_example/application.py`) demonstrating the structure for a new PyFlink Table API job.

## Cleaning Up

To remove the local virtual environment (if created) and stop/remove the Flink cluster containers:

```bash
make clean
```

## Disclaimer

Apache®, Apache Flink™, Flink™, and the Apache feather logo are trademarks of [The Apache Software Foundation](http://apache.org).