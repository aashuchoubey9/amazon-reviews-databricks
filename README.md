# Amazon Reviews Analytics — Databricks Data Engineering Project

An end-to-end **Data Engineering project built using Databricks** to ingest, process, clean, transform, and analyze Amazon product review data.

The project demonstrates a modern **Lakehouse architecture** using **Unity Catalog, Databricks Volumes, Auto Loader, Delta Lake, Lakeflow Declarative Pipelines, Lakeflow Jobs, Databricks SQL, GitHub, and GitHub Actions**.

## Project Objective

The objective of this project is to build a scalable and maintainable data pipeline that transforms raw Amazon review and product metadata JSON files into curated analytical datasets.

The pipeline follows a **Bronze → Silver → Gold** architecture:

Amazon Reviews JSON
        │
        ▼
Unity Catalog Volume
        │
        ▼
Auto Loader
        │
        ▼
     BRONZE
Raw review & product metadata
        │
        ▼
     SILVER
Cleaning + Deduplication + Data Quality
        │
        ▼
      GOLD
Business & analytical datasets
        │
        ├── Product Analytics
        ├── Brand Analytics
        ├── Rating Analytics
        ├── Review Trends
        ├── Reviewer Analytics
        └── Helpfulness Analytics

## Key Technologies

- **Databricks**
- **Unity Catalog**
- **Databricks Volumes**
- **Auto Loader**
- **Delta Lake**
- **PySpark**
- **Lakeflow Declarative Pipelines**
- **Lakeflow Jobs**
- **Databricks SQL**
- **GitHub**
- **Databricks Repos**

## Dataset

The project uses Amazon product review data and corresponding product metadata in JSON format.
Source - https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/

### Input Files

1. Magazine_Subscriptions.json
2. meta_Magazine_Subscriptions.json

The review data contains information such as:

- Product (`asin`)
- Reviewer information
- Rating
- Review text
- Review summary
- Review date
- Verified purchase status
- Helpful votes
- Brand
- Product category
- Product attributes

The product metadata contains product-level information such as:

- Product (`asin`)
- Brand
- Product category
- Product attributes

The two datasets can be related using the `asin` product identifier.

-----------------------------------------------------------------------------------------------------

# The Medallion Architecture

The project follows the Databricks **Medallion Architecture**.

## Bronze Layer - The Bronze layer stores the raw source data with minimal transformation.

### Ingestion

The raw JSON files are stored in a Unity Catalog Volume:
/Volumes/workspace/amazon_reviews/raw_reviews/

Databricks **Auto Loader** is used to ingest the JSON data.

### Bronze Tables

1. reviews_bronze
2. product_metadata_bronze

The Bronze layer also records ingestion metadata such as:

_ingested_at
_source_file

This provides traceability back to the source files.

## Silver Layer - The Silver layer prepares the Bronze data for reliable downstream analytics.

### Silver Processing

The pipeline performs following:

- String trimming and standardization
- Data type conversions
- Review deduplication
- Basic data-quality checks
- Data-quality flag creation
- Preservation of source information

### Silver Tables

reviews_cleaned
reviews_deduplicated
reviews_quality

### Deduplication

Reviews are deduplicated using a composite business key based on:

asin
reviewerID
unixReviewTime
reviewText

The original dataset contained:- 93,074 rows.

After deduplication:- 90,803 rows remained.

Therefore: 2,271 duplicate records were removed.

### Data Quality

Instead of deleting problematic records, the pipeline adds explicit data-quality flags.

Examples include:

dq_missing_asin
dq_missing_reviewer
dq_missing_rating
dq_invalid_rating
dq_missing_review_text
dq_missing_unix_time
dq_has_issue

This allows downstream consumers to identify problematic records while preserving the underlying data.

## Gold Layer - The Gold layer contains business-oriented analytical datasets designed for reporting and analysis.

### Gold Tables

product_review_metrics
brand_review_metrics
rating_distribution
review_trends
product_rating_distribution
product_review_summary
brand_rating_distribution
reviewer_activity_metrics
category_review_metrics
product_performance_metrics
review_helpfulness_metrics

These tables support analysis of:

- Product performance
- Brand performance
- Rating distribution
- Review trends over time
- Reviewer activity
- Product categories
- Review helpfulness
- Positive and negative rating patterns
- **GitHub Actions**
- **Python**
- **SQL**
-----------------------------------------------------------------------------------------------------

# Pipeline Engineering & Orchestration

## Lakeflow Declarative Pipeline

The Bronze, Silver, and Gold transformations are organized using a **Lakeflow Declarative Pipeline**.

The pipeline defines the dependencies between datasets and manages the execution of the data transformation flow.

Bronze
  │
  ▼
Silver
  │
  ▼
Gold

The pipeline includes:

1. Bronze ingestion using Auto Loader
2. Silver data cleaning
3. Silver deduplication
4. Data-quality flag generation
5. Gold analytical transformations
6. Dataset dependency management
7. Pipeline execution monitoring
8. Unity Catalog lineage

# Lakeflow Jobs - A **Lakeflow Job** is used to orchestrate the ETL pipeline.

The Job contains a task that triggers the existing ETL pipeline.

Daily Schedule
      │
      ▼
Lakeflow Job
      │
      ▼
run_etl_pipeline
      │
      ▼
Lakeflow Declarative Pipeline
      │
      ▼
Bronze → Silver → Gold

The Job was tested manually and completed successfully before enabling the daily schedule. Job run history is used to verify successful executions.

# Data Lineage - Unity Catalog lineage is used to visualize dependencies between datasets.

The lineage allows the relationship between upstream and downstream datasets to be traced through the pipeline.

Example:

Raw Sources
    │
    ▼
Bronze
    │
    ▼
Silver
    │
    ▼
Gold

This improves data discoverability and helps understand the impact of changes to upstream datasets.

# Git & Version Control

The project source code is maintained using **GitHub** and **Databricks Repos**.

Repository: amazon-reviews-databricks

The repository contains the Bronze, Silver, and Gold pipeline code.

Project structure:

amazon-reviews-databricks/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── src/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── README.md

# Continuous Integration - GitHub Actions is used to perform a basic CI validation whenever changes are pushed to the `main` branch or submitted through a pull request.

The CI workflow:

1. Checks out the repository.
2. Sets up Python 3.11.
3. Compiles the Python source files to detect syntax errors.

Developer
    │
    ▼
Git Push / Pull Request
    │
    ▼
GitHub Actions
    │
    ├── Checkout
    ├── Python 3.11
    └── Python Syntax Validation
             │
             ▼
        Success / Failure

This provides an automated quality check before changes are considered ready.

> **Note:** The current CI workflow performs source-code validation. Automated deployment of Databricks resources is intentionally outside the scope of this project.

-----------------------------------------------------------------------------------------------------

# Project Structure

amazon-reviews-databricks/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── src/
│   ├── bronze/
│   │   ├── reviews_bronze.py
│   │   └── product_metadata_bronze.py
│   │
│   ├── silver/
│   │   ├── reviews_cleaned.py
│   │   ├── reviews_deduplicated.py
│   │   └── reviews_quality.py
│   │
│   └── gold/
│       ├── product_review_metrics.py
│       ├── brand_review_metrics.py
│       ├── rating_distribution.py
│       ├── review_trends.py
│       ├── product_rating_distribution.py
│       ├── product_review_summary.py
│       ├── brand_rating_distribution.py
│       ├── reviewer_activity_metrics.py
│       ├── category_review_metrics.py
│       ├── product_performance_metrics.py
│       └── review_helpfulness_metrics.py
│
└── README.md

# Key Engineering Decisions

1. Unity Catalog

Unity Catalog is used to organize and govern the project's data assets.

The project uses:

Catalog
└── workspace
    └── Schema
        └── amazon_reviews

Tables and the raw-data Volume are organized within this namespace.

2. Databricks Volume for Raw Data

Raw JSON files are stored in a Unity Catalog Volume rather than directly mixing raw files with managed tables.

/Volumes/workspace/amazon_reviews/raw_reviews/

This provides a clearly defined landing location for the source data.

3. Auto Loader for Ingestion

Auto Loader is used for file ingestion because it is designed for incremental file discovery and scalable ingestion in Databricks.

The project uses: cloudFiles with JSON as the source format.

4. Bronze Layer — Preserve Raw Data

The Bronze layer performs minimal transformation. The objective is to retain the source information while adding ingestion metadata.
This makes the Bronze layer useful as the initial historical landing layer.

5. Silver Layer — Clean and Reliable Data

The Silver layer is responsible for preparing data for downstream analytics.

The project performs:

Raw data
   ↓
Standardization
   ↓
Deduplication
   ↓
Data-quality flags
   ↓
Analytics-ready Silver data

Problematic records are flagged rather than immediately discarded.

6. Composite Key for Review Deduplication

`asin` alone cannot uniquely identify a review because a product can have many reviews.

The project therefore uses a composite key consisting of:

asin
reviewerID
unixReviewTime
reviewText

This provides a more appropriate business-level basis for identifying duplicate review records.

7. Unix Timestamp for Review Trends

The source `reviewTime` field was not consistently suitable for date parsing.
Therefore, `unixReviewTime` is used for reliable time-based analysis.
The Gold `review_trends` dataset derives monthly trends from the Unix timestamp.

This provides a consistent basis for analyzing review activity over time.

8. Product Metadata Enrichment

The review dataset does not consistently contain brand information for rated reviews.
Product metadata is therefore used to enrich review analytics by joining on:

asin

This enables reliable brand-level analysis.

9. Gold Layer — Business-Oriented Metrics

The Gold layer converts cleaned data into datasets designed for analytical consumption.

Examples include:

Product metrics
Brand metrics
Rating distribution
Review trends
Reviewer activity
Category metrics
Product performance
Review helpfulness

This separates business-facing analytical logic from raw ingestion and cleaning logic.

# Project Outcome

The completed project demonstrates an end-to-end Databricks data engineering workflow:

Source JSON
     │
     ▼
Unity Catalog Volume
     │
     ▼
Auto Loader
     │
     ▼
Bronze Delta
     │
     ▼
Silver Delta
     │
     ├── Cleaning
     ├── Deduplication
     └── Data Quality
     │
     ▼
Gold Delta
     │
     ▼
Databricks SQL
     
Orchestration:
Lakeflow Jobs
     │
     ▼
Lakeflow Pipeline

Source Control:
Databricks Repos
     │
     ▼
GitHub
     │
     ▼
GitHub Actions CI

#THE FINAL SUMMARY:-

This project demonstrates practical experience with:

- Databricks Lakehouse architecture
- Unity Catalog
- Databricks Volumes
- Auto Loader
- Delta Lake
- PySpark
- Bronze/Silver/Gold architecture
- Data cleaning and transformation
- Deduplication
- Data-quality checks
- Analytical data modeling
- Lakeflow Declarative Pipelines
- Lakeflow Jobs
- Scheduling
- Unity Catalog lineage
- Databricks SQL
- Git and GitHub
- Databricks Repos
- GitHub Actions CI
