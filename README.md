# Enterprise-Grade Azure Databricks ETL Pipeline  
### Medallion Architecture • Lakeflow • Unity Catalog • Olist E-Commerce Analytics

## Overview
This repository contains a full end-to-end ETL data engineering project designed to replicate a real production-grade data platform. The pipeline ingests raw data from Azure Data Lake Storage Gen2, processes it with Azure Databricks (PySpark + SQL), enforces governance with Unity Catalog, and orchestrates the workflow using Lakeflow Declarative Pipelines.

The project follows the Medallion Architecture—Bronze, Silver, and Gold—to deliver a robust, scalable, and analytics-ready data solution.

The dataset used is the **Brazilian Olist E-Commerce Public Dataset**, containing real transactional records for orders, payments, deliveries, products, sellers, and reviews.

This project demonstrates the full lifecycle of a modern data engineering workflow and showcases how data can be transformed into business insights that support decision-making and machine learning use cases.

---

## Architecture

### Cloud Platform
- **Azure**
  - Azure Databricks (Unity Catalog enabled)
  - ADLS Gen2 (bronze, silver, gold containers)
  - Databricks Lakeflow (pipeline orchestration)

### Tech Stack
- **Languages:** Python, SQL, PySpark  
- **Data Governance:** Unity Catalog  
- **Orchestration:** Databricks Lakeflow (Declarative Pipelines)  
- **Data Architecture:** Medallion (Bronze → Silver → Gold)

---

## Pipeline Flow

### 1. Bronze Layer – Raw Ingestion
- Ingests raw CSV files from ADLS Gen2 source container.
- Stores them into the **bronze** schema with minimal processing.
- Schema inference and raw auditing (ingestion timestamp, source path).

### 2. Silver Layer – Cleansed & Conformed
- Data is validated, standardized, and cleaned.
- Handles missing values, date normalization, referential alignment.
- Each Olist table is transformed using modular Python/PySpark scripts.
- Outputs clean, analytics-ready **silver** tables.

### 3. Gold Layer – Star Schema + KPIs
- Modeled into **fact** and **dimension** tables.
- Built using SQL pipelines referencing silver tables.
- Implements:
  - Data quality expectations (null checks, value ranges, FK integrity)
  - Business metrics & KPIs
  - Optimized tables for BI dashboards and ML workflows

---

## Business Value Added

The Gold layer delivers actionable insights for decision-making:

### Key Metrics
- Total sales revenue  
- Order conversion rate  
- Delivery SLA performance  
- Product category profitability  
- Seller performance KPIs  
- Customer repeat behavior  
- Review sentiment progress  

### Use Cases Enabled
- Operational monitoring  
- Marketing segmentation  
- Forecasting (inventory, sales)  
- Customer experience analysis  

This transforms raw e-commerce data into measurable value that supports strategic and operational decisions.

---

## Repository Structure

