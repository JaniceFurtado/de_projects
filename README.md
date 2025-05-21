# Data Engineering Project

This project is to help data engineers to build data pipeline by generating random streaming data to kafka and storing it into Postgres Table.

### To start the cluster and create containers
    docker compose build

#### Once Build is complete
    docker compose up

## Overview

Step-by-Step:
 1. Generating Data
 2. Writing and consuming from Kafka
 3. Writing to Postgres tables
 4. Pyspark streaming and transforming data