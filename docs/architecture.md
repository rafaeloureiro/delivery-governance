# Technical Architecture — Delivery Governance

## 1. Architecture Overview

Delivery Governance is an end-to-end data engineering project built on Databricks.

The solution simulates a food delivery operation and demonstrates how data ingestion, transformation, quality validation, governance and analytics can be integrated into a single workflow.

The architecture follows the Medallion Architecture, organizing data into three layers:

- **Bronze:** raw data ingestion.
- **Silver:** data validation, cleansing and rejection handling.
- **Gold:** business-ready analytical datasets.

The implementation uses Python, PySpark, SQL, Delta Lake and Unity Catalog.

## 2. Data Pipeline

### 2.1 Bronze Layer

The Bronze layer stores the original synthetic order records.

Main table:

`delivery_governance.bronze.pedidos_raw`

A total of 100 synthetic orders are generated for the demonstration.

### 2.2 Silver Layer

The Silver layer applies data validation rules and separates valid records from rejected ones.

Main tables:

- `delivery_governance.silver.pedidos_validos`
- `delivery_governance.silver.pedidos_rejeitados`

The validation process identifies issues such as:

- Negative order values.
- Missing customer email.
- Invalid order status.
- Missing restaurant information.
- Missing customer identifiers.

The result is 95 valid records and 5 rejected records.

### 2.3 Gold Layer

The Gold layer contains analytical datasets prepared for reporting and monitoring.

Main tables:

- `delivery_governance.gold.indicadores_delivery`
- `delivery_governance.gold.qualidade_resumo`
- `delivery_governance.gold.qualidade_por_motivo`

These datasets support operational analysis and data quality monitoring.

A masked analytical view, `delivery_governance.gold.pedidos_analiticos`, was also developed to demonstrate personal data protection.

## 3. Data Quality

Data quality controls are applied during the Silver transformation process.

The pipeline identifies invalid records and stores them separately instead of silently discarding them.

This approach provides:

- Visibility into data quality issues.
- Traceability of
