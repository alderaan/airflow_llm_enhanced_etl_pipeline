# Introduction
This is a demo for an Extract-Transform-Load Pipeline with LLM-based Data Enrichment. 

In this project, we extract 100,000 eCommerce transactions and enrich them with OpenAI's Batch API. Data is ingested from normalized sources and automatically cleansed before being stored in a state-of-the-art data lake on Google Cloud Storage. From there, it is loaded into Google BigQuery—a petabyte-scale, enterprise-grade data warehouse organized into staging, clean, and aggregated layers. 

Apache Airflow orchestrates the complex ETL workflows, while integration with the OpenAI Batch API enriches tens of thousands of data points. Notably, customer reviews originally in Portuguese are translated into English and analyzed to extract precise numerical sentiment scores, transforming raw text into actionable insights.

These sentiment scores are then available for numerical analysis. For example, it becomes possible to track "delivery speed", "customer service quality" and other categories over time. All of these are extracted semantically from the customers reviews, leveraging a large language model in batch mode. 

For more information on the project, check out https://www.correlion.ai/en/portfolio/ai-enhanced-e-commerce-pipeline

# Installation
download dataset from https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data and extract csv filesto /source_data/ 

run ```make full-setup```

then run ```make airflow-start```

Airflow user: admin/admin