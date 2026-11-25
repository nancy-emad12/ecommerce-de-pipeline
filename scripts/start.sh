#!/usr/bin/env bash
# Build the Spark cluster image (Astro does not build images from docker-compose.override.yml),
# then start Airflow + Spark + DWH Postgres.
set -euo pipefail
cd "$(dirname "$0")/.."
docker build -t ecommerce-spark:4.0.1 ./docker/spark
astro dev start
