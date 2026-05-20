# World Flight Tracker

A personal portfolio project demonstrating an end-to-end modern data engineering pipeline built entirely using free-tier infrastructure and open-source tooling. This system ingests, stores, transforms, and serves approximately 6.5 million rows of real-time global flight data across a Medallion Architecture spanning PostgreSQL, Amazon S3, and Delta Lake on Databricks.

## Application URL

### [Streamlit App](https://worldflighttracker-k983uonnz4ybjhz9ahfnxl.streamlit.app/)

> Note: Due to Databricks Community Edition limitations, the SQL warehouse must occasionally be started manually. If the dashboard appears empty, the warehouse is likely inactive.

---

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION (Home Server)                        │
│                                                                             │
│   OpenSky Network API  ──►  Python Ingestor  ──►  PostgreSQL (Bronze Layer) │
│   REST API Polling           JSON Processing         Raw JSON Storage       │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER (PostgreSQL)                            │
│                                                                             │
│   Flatten JSON  ──►  Normalize & Clean  ──►  Structured Relational Tables   │
│   Parse Fields         Type Casting             Data Mart Models            │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER (Amazon S3)                             │
│                                                                             │
│   Parquet Export  ──►  S3 Bucket  ──►  Delta Lake (Databricks)              │
│   Every 5 Minutes       Cloud Storage        Analytical Views               │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          APPLICATION LAYER                                  │
│                                                                             │
│   Databricks Delta Read  ──►  Streamlit App  ──►  Live Flight Map           │
│   Incremental Refreshes        Analytical UI         Real-Time Visualization│
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Medallion Architecture

### Bronze Layer — Raw Ingestion (PostgreSQL)

- Ingests flight telemetry data from the **OpenSky Network REST API**
- Data is stored in PostgreSQL using raw JSON storage patterns for replayability and source fidelity
- Home server handles all ingestion and polling logic through Python-based ETL workflows
- Captures approximately **6.5 million rows** of raw flight state data on average
- Acts as the operational landing zone and immutable source of truth for downstream processing

### Silver Layer — Flattened & Normalized (PostgreSQL)

- Raw JSON records are flattened and transformed into structured relational datasets
- Type casting, null handling, and schema normalization are applied during transformation workflows
- Data marts are created at this layer to support downstream analytical use cases:
  - Flight status and position tracking
  - Airline and origin aggregations
  - Altitude and velocity analytics

### Gold Layer — Analytical Store (S3 + Databricks Delta Lake)

- Processed datasets are exported to Amazon S3 as partitioned Parquet files every 5 minutes
- S3 acts as the bridge between home-server processing and cloud analytics workloads
- Databricks consumes the Parquet datasets as Delta Lake tables, enabling:
  - ACID transactions
  - Schema enforcement
  - Time-travel queries
  - Incremental analytical refreshes
- Incremental reads in Databricks provide a near-real-time analytical layer for downstream reporting and visualization

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Data Source | OpenSky Network REST API |
| Ingestion Runtime | Linux Home Server |
| Raw Storage (Bronze) | PostgreSQL — JSON Storage |
| Transformation (Silver) | Python, SQL — JSON Flattening & Data Mart Construction |
| Cloud Storage | Amazon S3 |
| Analytical Engine | Databricks & Delta Lake |
| File Format | Apache Parquet |
| Serving Layer | Streamlit |
| Language | Python |

---

## Why It's Split This Way

This architecture was intentionally designed to remain fully operational within free-tier infrastructure constraints while still replicating modern enterprise data engineering patterns.

- Home server ingestion avoids continuous cloud compute costs for API polling workloads
- PostgreSQL acts as an operational landing zone for raw telemetry ingestion and transformation
- Amazon S3 bridges on-premise processing with cloud-based analytics
- Databricks Community Edition provides access to Spark and Delta Lake workflows without managed infrastructure costs
- Parquet and Delta Lake simulate a modern lakehouse architecture using open storage patterns

Despite operating entirely on free-tier infrastructure, the system demonstrates architectural concepts commonly used in production environments, including Medallion Architecture, columnar storage, incremental processing, and cloud analytical serving.

---

## Scale & Performance

- Approximately **6.5 million rows** ingested and processed on average
- Parquet exports refresh every **5 minutes** for near-real-time analytical updates
- Delta Lake provides schema enforcement and time-travel support on the analytical layer
- Columnar Parquet storage improves compression efficiency and downstream query performance

---

## Repository Structure

```text
world_flight_tracker/
├── app/             # Streamlit application layer
├── etl/             # ETL workflows: ingestion, transformation, export
├── sql_queries/     # SQL queries for analytical data marts
├── logs/            # Pipeline execution logs
├── config.yaml      # Pipeline configuration
└── requirements.txt
```

---

## Portfolio Purpose

This project was built to demonstrate:

- End-to-end pipeline ownership from ingestion to analytical serving
- Practical implementation of Medallion Architecture
- Modern lakehouse tooling using Delta Lake and Parquet
- Problem-solving within free-tier infrastructure constraints
- High-volume telemetry ingestion and transformation workflows
- Cloud-based analytical serving using Databricks

---

## Contact

**Jeremy McCormick** — Data Engineer  
JeremyAlanMcCormick@gmail.com  

- [LinkedIn](https://www.linkedin.com/in/jeremyalanmccormick/)
- [GitHub](https://github.com/SpotMcCormick)