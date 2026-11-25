# Build the Spark cluster image (Astro does not build images from docker-compose.override.yml),
# then start Airflow + Spark + DWH Postgres.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
docker build -t ecommerce-spark:4.0.1 ./docker/spark
if ($LASTEXITCODE -ne 0) { throw "Spark image build failed" }
astro dev start
