# Data Observability Thesis Materials

This repository contains materials for my Master's thesis. The work is organized into datasets, a batch invoice-processing pipeline, and a streaming vehicle-position pipeline. Both pipelines include two observability approaches:

- **GE**: rule-based validation and monitoring with Great Expectations.
- **AIOps**: anomaly detection using feature engineering and autoencoder-based scoring (PyTorch + autoencoder).

## Repository Structure

```text
data-observability-thesis-materials/
|-- batch_data_pipeline/
|   |-- AIOps/
|   |-- GE/
|   |-- azure_doc_intelligence/
|   `-- aiops_rule_compare_results.ipynb
|-- dataset/
|-- streaming_data_pipeline/
|   |-- AIOps/
|   |-- GE/
|   |-- MQTT/
|   `-- compare_rule_aiops_results.ipynb
`-- README.md
```

## `dataset/`

This folder contains the input and training datasets used by the batch invoice pipeline.

| File | Purpose |
| --- | --- |
| `invoices_1000_items.csv` | Structured invoice item dataset used for batch processing, training, validation, and comparison experiments. |
| `clean_invoice_training_data.rar` | Archived cleaned invoice training data used by the AIOps batch notebooks. |
| `1000+ PDF_Invoice_Folder.zip` | Archived PDF invoice source documents used as input for Azure Document Intelligence extraction. |

## `batch_data_pipeline/`

This folder contains the batch-processing experiment for invoice data. It covers document extraction, Bronze/Silver/Gold lakehouse processing, rule-based data quality checks, AIOps model training/scoring, and comparison between the two detection approaches.

### `batch_data_pipeline/azure_doc_intelligence/`

| File | Purpose |
| --- | --- |
| `analyze_batch_documents.py` | Runs Azure Document Intelligence batch analysis with the `prebuilt-invoice` model. It reads input/output Azure Blob SAS URLs and service credentials from environment variables, submits the batch job, and prints succeeded, failed, and skipped document counts. |

### `batch_data_pipeline/GE/`

These notebooks implement the rule-based batch pipeline with Great Expectations checks at the Bronze, Silver, and Gold stages.

| Notebook | Purpose |
| --- | --- |
| `bronze_ingest.ipynb` | Ingests raw invoice extraction output into the Bronze layer and attaches ingestion metadata such as source file and timestamp. |
| `bronze_ge.ipynb` | Applies Great Expectations validations to Bronze invoice data and records early-stage quality issues. |
| `silver_transform.ipynb` | Cleans and standardizes Bronze invoice data into the Silver layer, including parsing, trimming, deduplication, and normalization logic. |
| `silver_ge.ipynb` | Applies Great Expectations validations to Silver data after cleansing and transformation. |
| `gold_publish.ipynb` | Publishes validated Silver data into Gold serving outputs for analysis/reporting. |
| `gold_ge.ipynb` | Audits Gold outputs with Great Expectations to confirm final serving-layer quality. |

### `batch_data_pipeline/AIOps/`

These notebooks implement the machine-learning observability path for the batch invoice pipeline.

| Notebook | Purpose |
| --- | --- |
| `invoice_training_handle.ipynb` | Prepares invoice training data for the AIOps workflow using Spark transformations, cleanup, and standardization. |
| `aiops_silver_layer_feature_build.ipynb` | Builds Silver-layer features from cleaned invoice header and line data for anomaly-detection training and scoring. |
| `aiops_train_autoencoder.ipynb` | Trains a PyTorch autoencoder model for invoice anomaly detection. |
| `aiops_score_silver.ipynb` | Scores Silver invoice records with the trained autoencoder and produces anomaly indicators. |

### Batch Comparison Notebook

| Notebook | Purpose |
| --- | --- |
| `aiops_rule_compare_results.ipynb` | Compares AIOps anomaly detections with rule-based Great Expectations detections. It analyzes overlap, AIOps-only records, rule-only records, severity coverage, source-type coverage, and error agreement. |

## `streaming_data_pipeline/`

This folder contains the streaming experiment for HSL vehicle-position data. It includes MQTT ingestion, Event Hubs publishing, streaming Bronze/Silver/Gold processing, Great Expectations validation, AIOps record-level anomaly scoring, and comparison reporting.

### `streaming_data_pipeline/MQTT/`

| File | Purpose |
| --- | --- |
| `hsl_hfp_to_eventhub.py` | Subscribes to the HSL MQTT broker, parses topic metadata and JSON payloads, wraps messages in an ingestion envelope, batches them, and sends them to Azure Event Hubs with retry handling. |

### `streaming_data_pipeline/GE/`

These notebooks implement the rule-based streaming pipeline with Great Expectations validation at each data layer.

| Notebook | Purpose |
| --- | --- |
| `bronze_ingest_stream.ipynb` | Reads Event Hubs data as a Kafka stream and writes raw vehicle-position messages into the Bronze raw table with checkpointing and monitoring. |
| `bronze_route_stream.ipynb` | Parses raw Bronze envelopes, separates valid records from quarantine records, and writes batch-level routing metrics. |
| `bronze_validate_ge.ipynb` | Applies Great Expectations checks to streaming Bronze records. |
| `silver_transform_stream.ipynb` | Transforms validated Bronze vehicle-position records into cleaned Silver streaming tables. |
| `silver_validate_ge.ipynb` | Applies Great Expectations checks to Silver streaming records. |
| `gold_publish_stream.ipynb` | Publishes Silver validated data into Gold serving tables such as current positions and route-level aggregations. |
| `gold_validate_ge.ipynb` | Applies Great Expectations checks to Gold streaming outputs. |
| `ge_detection_report.ipynb` | Produces reports for rule-based GE detections, including layer summaries, detection matrices, error/severity analysis, detail tables, and runtime summaries. |

### `streaming_data_pipeline/AIOps/`

These notebooks implement the AIOps observability path for streaming vehicle-position data.

| Notebook | Purpose |
| --- | --- |
| `training_bronze_ingest_route_stream.ipynb` | Builds the training Bronze stream and routes valid/quarantined HSL vehicle-position records for model-training data preparation. |
| `training_silver_clean_stream.ipynb` | Cleans training Bronze data into the training Silver table. |
| `training_gold_publish_stream.ipynb` | Publishes cleaned training Silver data into training Gold tables, including current and route-window outputs. |
| `aiops_build_record_features.ipynb` | Builds record-level features for AIOps scoring and reporting. |
| `aiops_train_autoencoder.ipynb` | Trains PyTorch record-level autoencoders for streaming anomaly detection. |
| `aiops_bronze_route_stream.ipynb` | Routes AIOps Bronze streaming records into valid, quarantine, and metrics outputs. |
| `aiops_score_bronze_record.ipynb` | Scores Bronze-level records with the trained autoencoder and produces anomaly scores. |
| `aiops_silver_transform_stream.ipynb` | Transforms AIOps-validated Bronze records into cleaned Silver streaming outputs. |
| `aiops_score_silver_record.ipynb` | Scores Silver-level records for anomalies. |
| `aiops_gold_publish_stream.ipynb` | Publishes AIOps Silver outputs into Gold serving tables. |
| `aiops_score_gold_record.ipynb` | Scores Gold-level records for anomalies. |
| `aiops_detection_report.ipynb` | Produces AIOps detection reports, including layer summaries, detection matrices, severity analysis, contributing features, source-type coverage, and runtime summaries. |

### Streaming Comparison Notebook

| Notebook | Purpose |
| --- | --- |
| `compare_rule_aiops_results.ipynb` | Compares rule-based GE detections with AIOps detections for the streaming pipeline. It summarizes overlap, method-specific detections, severity, rule coverage, and AIOps-only/rule-only cases. |

## Notes

- Most notebooks are designed for a Spark/Databricks-style environment and reference Azure Data Lake Storage paths, Unity Catalog tables, widgets, checkpoints, Delta tables, and Great Expectations.
