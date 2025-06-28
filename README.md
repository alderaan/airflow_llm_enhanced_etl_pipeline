# 🚀 AI-Enhanced E-commerce Data Pipeline

A comprehensive Extract-Transform-Load (ETL) pipeline that processes 100,000+ e-commerce transactions with advanced LLM-based data enrichment capabilities. This project demonstrates modern data engineering practices using Apache Airflow, Google Cloud Platform, and OpenAI's Batch API.

## 🌟 Key Features

- **Multi-stage Data Processing**: Staging → Clean → Aggregated data layers
- **LLM-Powered Enrichment**: Automated translation and sentiment analysis using OpenAI
- **Cloud-Native Architecture**: Google Cloud Storage + BigQuery integration
- **Production-Ready Orchestration**: Apache Airflow with custom operators
- **Local Development Setup**: Complete Airflow installation with PostgreSQL backend

## 📊 Data Flow Overview

This pipeline processes Brazilian e-commerce data from Olist, enriching customer reviews with:
- **Automatic Translation**: Portuguese → English using GPT-3.5-turbo
- **Aspect-Based Sentiment Analysis**: Numerical scores for delivery, product quality, customer service, refund process, and packaging
- **Advanced Analytics**: Time-series analysis and geographic insights

The enriched data enables sophisticated business intelligence, allowing tracking of specific service aspects over time and across different regions.

## 🔗 Project Information

For more details about this project, visit: [Correlion AI Portfolio](https://www.correlion.ai/en/portfolio/ai-enhanced-e-commerce-pipeline)

## 📋 Prerequisites

### System Requirements
- **Python 3.8+** (recommended: Python 3.8)
- **PostgreSQL 14+** (for Airflow metadata database)
- **macOS/Linux** (tested on macOS with Homebrew)
- **8GB+ RAM** (recommended for smooth operation)

### Cloud Services
- **Google Cloud Platform** account with:
  - BigQuery enabled
  - Cloud Storage enabled
  - Service account with appropriate permissions
- **OpenAI API** account with:
  - API key configured
  - Batch API access

### Data Source
- **Olist Brazilian E-commerce Dataset** from Kaggle:
  - Download from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data
  - Extract CSV files to `/source_data/` directory

## 🛠️ Installation & Setup

This project includes a complete local Apache Airflow installation with PostgreSQL backend. We use a Makefile to simplify common operations.

### 1. Quick Setup (Recommended)

```bash
# Download and extract the dataset to /source_data/
# Then run the complete setup
make full-setup
```

### 2. Manual Setup Steps

If you prefer step-by-step installation:

```bash
# Install dependencies and create virtual environment
make install

# Create PostgreSQL database
make create-db

# Initialize Airflow with local configuration
make init
```

### 3. Start the Pipeline

```bash
# Start Airflow webserver and scheduler
make airflow-start
```

**Access Airflow UI**: http://localhost:8080  
**Default credentials**: admin/admin

### 4. Available Make Commands

| Command | Description |
|---------|-------------|
| `make full-setup` | Complete installation (clean + install + create-db + init) |
| `make install` | Install Python dependencies and create virtual environment |
| `make create-db` | Create PostgreSQL database for Airflow |
| `make init` | Initialize Airflow configuration and create admin user |
| `make airflow-start` | Start Airflow webserver and scheduler |
| `make airflow-stop` | Stop all Airflow processes |
| `make clean` | Remove all local files and database |
| `make kernel` | Register Jupyter kernel for notebook development |

## 🏗️ Pipeline Architecture

### Data Flow Stages

The pipeline follows a modern data architecture with three distinct layers:

```
📁 Source Data (CSV) 
    ↓
🌊 Staging Layer (BigQuery)
    ↓
🧹 Clean Layer (BigQuery) 
    ↓
🤖 Enrichment Layer (LLM Processing)
    ↓
📊 Aggregated Layer (BigQuery)
```

### 1. **Staging Layer** (`olist_staging`)
- **Purpose**: Raw data ingestion from CSV files
- **Tables**: customers, geolocation, order_items, order_payments, orders, products, sellers, product_category_name_translation
- **Operators**: `LocalFilesystemToGCSOperator` → `GCSToBigQueryOperator`
- **Process**: CSV → Google Cloud Storage → BigQuery with auto-detection

### 2. **Clean Layer** (`olist_clean`)
- **Purpose**: Data quality and standardization
- **Transformations**:
  - **Pass-through tables**: Simple SELECT * operations for most tables
  - **Order Reviews**: Deduplication and column preparation for LLM enrichment
- **Operators**: `BigQueryInsertJobOperator` with custom SQL
- **Special handling**: Adds `review_comment_message_en` and `review_aspect_scores` columns

### 3. **Enrichment Layer** (LLM Processing)
- **Purpose**: AI-powered data enrichment using OpenAI Batch API
- **Custom Operators**:
  - `ReviewTranslationToGCSOperator`: Translates Portuguese reviews to English
  - `ReviewTranslationToBQOperator`: Loads translations back to BigQuery
  - `ReviewAspectScoringToGCSOperator`: Generates aspect-based sentiment scores
  - `ReviewAspectScoringToBQOperator`: Loads sentiment scores to BigQuery

#### LLM Enrichment Process:
1. **Translation**: GPT-3.5-turbo translates Portuguese reviews to English
2. **Aspect Scoring**: Analyzes reviews for 5 key aspects:
   - Delivery speed
   - Product quality  
   - Customer service
   - Refund process
   - Packaging condition
3. **Batch Processing**: Uses OpenAI's Batch API for efficient processing of 10,000+ reviews
4. **JSON Output**: Aspect scores stored as structured JSON for easy querying

### 4. **Aggregated Layer** (`olist_aggregated`)
- **Purpose**: Business intelligence and analytics
- **Tables**:
  - `agg_sales_daily`: Daily sales metrics and KPIs
  - `agg_sales_by_city`: Geographic sales analysis by month
  - `agg_aspect_score_by_product`: Product-level sentiment analysis

## 🔧 Technical Components

### Google Cloud Integration
- **BigQuery**: Enterprise data warehouse with automatic scaling
- **Cloud Storage**: Data lake for raw files and intermediate results
- **Service Account**: Secure authentication and authorization

### Custom Airflow Operators
- **Translation Operators**: Handle Portuguese → English translation workflow
- **Aspect Scoring Operators**: Process sentiment analysis with structured output
- **Batch Processing**: Efficient handling of large-scale LLM operations

### Data Transformations
- **Deduplication**: Removes duplicate reviews based on review_id and order_id
- **Schema Evolution**: Adds new columns for enriched data
- **Aggregation**: Creates business-ready summary tables
- **Geographic Analysis**: City and state-level insights
- **Time Series**: Monthly and daily trend analysis

### Configuration Management
- **YAML Configuration**: Centralized pipeline configuration in `etl_pipeline_config.yaml`
- **Environment Variables**: Airflow configuration for local development
- **Makefile**: Simplified command execution and process management

## 🚀 Usage Guide

### Running the Pipeline

1. **Start Airflow**:
   ```bash
   make airflow-start
   ```

2. **Access the UI**: Navigate to http://localhost:8080 (admin/admin)

3. **Trigger the DAG**: 
   - Find the `correlion_olist` DAG in the Airflow UI
   - Click "Trigger DAG" to start the pipeline
   - Monitor progress in the Graph view

### Pipeline Execution Flow

The pipeline executes in the following sequence:

1. **Staging Load** (Task Group): Uploads all CSV files to GCS and loads into BigQuery staging tables
2. **Data Cleaning** (Task Group): Processes and cleans data, preparing for enrichment
3. **Review Enrichment** (Task Group): 
   - Translates Portuguese reviews to English
   - Generates aspect-based sentiment scores
4. **Aggregations** (Task Group): Creates business intelligence tables

### Monitoring and Logs

- **Airflow Logs**: Available in the Airflow UI for each task
- **Local Logs**: Check `webserver.log` and `scheduler.log` for system-level issues
- **BigQuery**: Monitor data processing in the BigQuery console
- **OpenAI**: Track batch job progress in the OpenAI dashboard

## 📁 Project Structure

```
airflow_demo/
├── dags/                          # Airflow DAG definitions
│   ├── etl_pipeline.py           # Main ETL pipeline DAG
│   ├── etl_pipeline_config.yaml  # Pipeline configuration
│   └── operators/                # Custom Airflow operators
│       ├── review_translation.py      # Translation operators
│       └── review_aspect_scoring.py   # Sentiment analysis operators
├── notebooks/                    # Jupyter notebooks for analysis
├── source_data/                  # Raw CSV data files
├── tmp/                         # Temporary files for processing
├── Makefile                     # Build and deployment commands
├── requirements.txt             # Python dependencies
├── airflow-requirements.txt     # Airflow-specific dependencies
└── README.md                    # This file
```

## 🔧 Configuration

### Environment Setup

The pipeline requires the following environment variables:

```bash
# Google Cloud (set in Airflow connections)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# OpenAI (set in Airflow connections)
OPENAI_API_KEY=your_openai_api_key

# BigQuery Project
BQ_PROJECT=your_project_id
```

### Customizing the Pipeline

- **Data Sources**: Modify `etl_pipeline_config.yaml` to add new tables
- **LLM Prompts**: Update operators in `dags/operators/` for different enrichment tasks
- **Aggregations**: Add new aggregation queries in the main DAG file
- **Scheduling**: Change `schedule_interval` in the DAG configuration

## 🐛 Troubleshooting

### Common Issues

1. **Port 8080 Already in Use**:
   ```bash
   make airflow-stop
   make airflow-start
   ```

2. **PostgreSQL Connection Issues**:
   ```bash
   # Ensure PostgreSQL is running
   brew services start postgresql@14
   make create-db
   ```

3. **Google Cloud Authentication**:
   - Verify service account permissions
   - Check `GOOGLE_APPLICATION_CREDENTIALS` environment variable
   - Ensure BigQuery and Cloud Storage APIs are enabled

4. **OpenAI API Issues**:
   - Verify API key is valid and has batch access
   - Check rate limits and billing status
   - Monitor batch job status in OpenAI dashboard

5. **Memory Issues**:
   - Increase system RAM or reduce batch sizes
   - Monitor resource usage during pipeline execution

### Debugging Tips

- **Check Airflow Logs**: Detailed error messages in the Airflow UI
- **Verify Data**: Use BigQuery console to inspect table contents
- **Test Operators**: Run individual tasks to isolate issues
- **Monitor Resources**: Watch CPU/memory usage during execution

## 📈 Performance Considerations

- **Batch Size**: LLM processing limited to 10,000 reviews per batch
- **Processing Time**: Full pipeline typically takes 2-4 hours
- **Cost Optimization**: Monitor OpenAI API usage and costs
- **Scaling**: Pipeline designed to handle 100,000+ records efficiently

## 🤝 Contributing

This project demonstrates modern data engineering practices. For questions or contributions:

1. Review the code structure and documentation
2. Test changes in a local environment
3. Ensure all tests pass before submitting
4. Update documentation for any new features

## 📄 License

This project is part of the Correlion AI portfolio. For commercial use or licensing inquiries, please contact the project maintainers.