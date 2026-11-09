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
- Traceability of rejected records.
- Separation between analytical data and invalid records.
- A foundation for monitoring data reliability.

The project includes a dedicated notebook with 10 automated validation tests.

Latest execution results:

| Metric | Result |
|---|---:|
| Total tests | 10 |
| Passed | 10 |
| Failed | 0 |
| Overall result | Passed |

## 4. Orchestration and Testing

The pipeline execution is organized through a Databricks Job with two dependent tasks:

1. `gerar_dados` — generates synthetic data and updates the pipeline tables.
2. `testar_pipeline` — executes automated validation tests after the data generation task succeeds.

The dependency ensures that tests run only after the pipeline execution is completed.

Latest successful execution:

| Task | Execution time | Status |
|---|---:|---|
| Data generation | 1m 05s | Succeeded |
| Pipeline testing | 18s | Succeeded |
| Total | 1m 23s | Succeeded |

The Job is manually triggered. Recurring scheduling is not currently configured.

## 5. Governance and Security

Unity Catalog was used to configure and review data access permissions.

The `delivery_analysts` group received:

- `USE CATALOG` on the project catalog.
- `USE SCHEMA` on the Gold schema.
- `SELECT` on the three main Gold analytical tables.
- `Can View` access to the published dashboard.

Access to raw Bronze data and rejected Silver records was not granted to this group.

A masked analytical view was also developed to demonstrate the protection of personally identifiable information.

The permission configuration was reviewed in the Databricks workspace. Independent validation using a separate user identity remains pending.

## 6. Analytics and Monitoring

The project includes a published Databricks dashboard with two sections:

### Executive Overview

Provides operational indicators and delivery performance metrics.

### Quality and Observability

Provides visibility into data quality indicators and rejection reasons.

The dashboard uses analytical datasets from the Gold layer.

## 7. Implementation Scope and Limitations

The project demonstrates the integration of data engineering, quality validation, orchestration, governance and analytics.

Current limitations:

- All records are synthetic.
- Job execution is manually triggered.
- Recurring scheduling is not configured.
- Independent access validation with a separate identity remains pending.
- The automated tests can be expanded to cover additional edge cases.

## 8. Future Improvements

Potential next steps include:

- Configure scheduled pipeline execution.
- Expand automated data quality coverage.
- Validate permissions using a separate identity.
- Improve data lineage documentation.
- Expand operational indicators and monitoring.

---

**Project:** Delivery Governance  
**Platform:** Databricks  
**Author:** Rafael Pereira
