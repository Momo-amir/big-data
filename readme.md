## Big Data Analytics with Hadoop and Spark

This project demonstrates how to set up a big data analytics environment using Hadoop and Spark with Docker. It includes configurations for both Hadoop and Spark clusters, allowing you to run big data processing tasks efficiently.

copy the .env.example file to .env and update the environment variables as needed.

### Commands

_run scripts in the terminal to build and run the Docker containers, and to execute commands inside the container:_

```bash

# Copy the example environment file to .env this will be used to configure the Docker containers
cp .env.example .env

# Build and start the Docker containers in detached mode

docker compose up --build -d

# To start the containers without rebuilding, use:
docker compose up -d

```

http://localhost:9870/dfshealth.html#tab-overview to see the Hadoop cluster status.

http://localhost:8081/ to see the Spark cluster status.
