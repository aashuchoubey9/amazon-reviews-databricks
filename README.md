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
- **GitHub Actions**
- **Python**
- **SQL**
