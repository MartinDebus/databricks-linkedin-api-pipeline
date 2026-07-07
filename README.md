# Databricks LinkedIn Analytics Pipeline

This repository contains a complete walkthrough and implementation for building a LinkedIn analytics pipeline on Databricks. It covers project scaffolding, ingesting LinkedIn metrics via the API, transforming data through bronze/silver/gold medallion layers, and deploying the pipeline and dashboard using Declarative Automation Bundles.

Blog posts (multi-part walkthrough):

- **Part 1 — Project setup with Declarative Automation Bundles:** https://www.thelakehousepath.com/p/building-a-linkedin-analytics-pipeline-part1 — Describes the repository structure, bundle configuration, Unity Catalog schemas, and landing volume setup so you can deploy the scaffold with `databricks bundle deploy`.
- **Part 2 — Connecting to the LinkedIn API:** https://www.thelakehousepath.com/p/building-a-linkedin-analytics-pipeline-part2 — Shows how to get API access, authenticate, call the five key endpoints, handle token and error edge cases, and land raw JSON into a Databricks Volume securely.
- **Part 3 — Building the Medallion Pipeline:** https://www.thelakehousepath.com/p/building-a-linkedin-analytics-pipeline-part3 — Implements bronze/silver/gold transformations with Declarative Pipelines, CDC upserts, and a materialized `daily_metrics` table for analytics and dashboards.
- **Part 4 — Creating a Dashboard and Pipeline Cost:** https://www.thelakehousepath.com/p/building-a-linkedin-analytics-pipeline-part4 — Covers building the dashboard on top of the `daily_metrics` table, visualizing LinkedIn KPIs, and analyzing the cost of running the pipeline on Databricks.

Quick keywords for discoverability: Databricks LinkedIn analytics, LinkedIn API ingest, Declarative Automation Bundles, Unity Catalog, medallion architecture, Databricks pipelines, LinkedIn analytics pipeline.

Quick start

- Deploy the bundle from the repository root: `databricks bundle deploy --target dev-user`
- Run the ingest job or open the `notebooks/ingest.ipynb` to fetch LinkedIn metrics into the landing volume.

See the `projects/linkedin` folder for resource YAML and `src/pipelines` for SQL pipeline code.