# World Flight Tracker

A personal portfolio project demonstrating a **production-grade, end-to-end data engineering pipeline** built entirely for free using modern tooling. This system ingests, stores, transforms, and serves **~6.5 million rows of real-time global flight data** across a multi-layer medallion architecture — from a home server all the way to Databricks Delta Lake.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA INGESTION (Home Server)                        │
│                                                                             │
│   OpenSky Network API  ──►  Python Ingestor  ──►  PostgreSQL (Bronze Layer) │
│   (Free Tier)                 (REST JSON)           Raw JSON Storage         │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER (PostgreSQL)                            │
│                                                                              │
│   Flatten JSON  ──►  Normalize & Clean  ──►  Structured Relational Tables   │
│   Parse Fields         Type Casting             Data Mart Models             │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER (Amazon S3)                              │
│                                                                              │
│   Parquet Export  ──►  S3 Bucket  ──►  Delta Lake (Databricks)              │
│   Every 5 min           Free Tier        Analytical Views                    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         APPLICATION LAYER                                    │
│                                                                              │
│   Databricks Stream Read  ──►  Flask/Streamlit App  ──►  Live Flight Map    │
│   (Delta Lake)                  5-min Parquet Refresh    Real-Time UI        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Medallion Architecture

### Bronze Layer — Raw Ingestion (PostgreSQL)
- Ingests flight data from the **OpenSky Network REST API** (free tier)
- Data is stored **as-is in JSON format** inside PostgreSQL for full fidelity and replayability
- Home server handles all ingestion logic via Python scripts
- Captures **~6.5 million rows** of raw flight state data on average
- Acts as the immutable source of truth for all downstream layers

### Silver Layer — Flattened & Normalized (PostgreSQL)
- JSON records are **flattened and parsed** into structured relational tables
- Type casting, null handling, and field normalization applied
- **Data marts** are built at this layer, optimized for specific analytical use cases:
  - Flight status and position tracking
  - Airline/origin aggregations
  - Altitude and velocity analytics

### Gold Layer — Analytical Store (S3 + Databricks Delta Lake)
- Processed data is **exported to Amazon S3 as Parquet files every 5 minutes**
- S3 acts as the bridge between home-server processing and cloud analytics
- **Databricks reads the S3 Parquet files as a Delta Lake table**, enabling:
  - ACID transactions
  - Time travel queries
  - Scalable, schema-enforced analytical views
- Stream reads in Databricks provide a near-real-time analytical layer

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Data Source** | OpenSky Network REST API (Free Tier) |
| **Ingestion Runtime** | Home Server (Linux) |
| **Raw Storage (Bronze)** | PostgreSQL — JSON column storage |
| **Transformation (Silver)** | Python, SQL — JSON flattening, data mart construction |
| **Cloud Storage** | Amazon S3 (Free Tier eligible) |
| **Analytical Engine** | Databricks Community Edition + Delta Lake |
| **File Format** | Parquet (columnar, compressed) |
| **Serving / App** | Python (Flask / Streamlit) |
| **Language** | Python 100% |

---

## Why It's Split This Way

This architecture was designed to be **100% free** while still replicating enterprise-grade data engineering patterns:

- **Home server handles ingestion** — avoids cloud compute costs for continuous polling
- **PostgreSQL replaces a paid message broker** — stores raw JSON until transformation
- **S3 free tier** bridges on-premise to cloud without cost
- **Databricks Community Edition** provides a legitimate Spark + Delta Lake environment
- **Parquet on S3** mimics a real data lakehouse without Databricks-managed storage fees

Despite the zero-cost constraint, this system demonstrates the same architectural principles used in production at scale: **Medallion Architecture, Delta Lake, columnar storage, and stream-style reads**.

---

## Scale & Performance

- **~6.5 million rows** ingested and processed on average
- Parquet exports refresh **every 5 minutes**, enabling near-real-time analytics
- Delta Lake provides **schema enforcement and time-travel** on the analytical layer
- Columnar Parquet format ensures **efficient compression and fast query scans**

---

## Repository Structure

```
world_flight_tracker/
├── app/          # Application layer (UI, serving)
├── etl/          # ETL scripts: ingestion, flattening, export
├── logs/         # Pipeline run logs
├── adhoc.py      # Ad hoc analysis scripts
├── adhoc.sql     # Ad hoc SQL queries
├── config.yaml   # Pipeline configuration
├── data.csv      # Sample dataset
└── requirements.txt
```

---

## Getting Started

### Prerequisites
- Python 3.9+
- PostgreSQL
- AWS account (S3 bucket — free tier)
- Databricks Community Edition account
- OpenSky Network account (free)

### Installation

```bash
git clone https://github.com/SpotMcCormick/world_flight_tracker.git
cd world_flight_tracker
pip install -r requirements.txt
cp config.yaml config.local.yaml  # Add your API keys and DB credentials
```

### Running the Pipeline

```bash
# Step 1: Ingest from API into PostgreSQL (Bronze)
python etl/ingest.py

# Step 2: Flatten and build data marts (Silver)
python etl/transform.py

# Step 3: Export to S3 as Parquet (Gold)
python etl/export_to_s3.py

# Step 4: Launch the app
python app/app.py
```

---

## Portfolio Purpose

This project was built to demonstrate:

- **End-to-end pipeline ownership** — from raw API ingestion to analytical serving
- **Medallion architecture** applied outside of a paid enterprise environment
- **Modern data tooling** (Delta Lake, Parquet, Databricks) at zero cost
- **Problem-solving under constraints** — replicating enterprise patterns with free-tier resources
- **Real-world data volume** — 6.5M+ rows with real flight telemetry

---

## Contact

**Jeremy McCormick** — Data Engineer  
[JeremyAlanMcCormick@gmail.com](mailto:JeremyAlanMcCormick@gmail.com)  
[LinkedIn](https://www.linkedin.com/in/jeremyalanmccormick/)  
[GitHub](https://github.com/SpotMcCormick)