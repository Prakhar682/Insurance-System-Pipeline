
<img width="1303" height="868" alt="Screenshot 2026-01-21 215759" src="https://github.com/user-attachments/assets/803659d0-a21a-4ea4-a1dc-16e0364a14a8" />

This project implements a production-grade end-to-end Data Engineering pipeline for an Insurance System using Databricks Delta Live Tables (DLT) and Apache Spark (PySpark).

The pipeline follows the Medallion Architecture (Bronze → Silver → Gold) to ingest raw data, enforce data quality, apply business logic, and generate analytics-ready KPIs for decision-making.

This project is designed with a real-world enterprise mindset, focusing on data reliability, scalability, and business usability.

==== Architecture Overview ====

::: Bronze -> Silver -> Gold :::

1- Bronze Layer: Raw ingestion with schema preservation & data quality detection
2- Silver Layer: Cleansed, standardized, deduplicated, and conformed datasets
3- Gold Layer: Business KPIs and executive-level analytical metrics

