#  Production-Grade Automated ETL Pipeline: Apache Airflow, PostgreSQL & Power BI

[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

An end-to-end, production-grade automated Business Intelligence (BI) data pipeline. This project automates daily billing CSV data ingestion, cleaning, transformation, and loading into a Dockerized PostgreSQL Data Warehouse via Apache Airflow, seamlessly powering automated reports in Power BI Service using an On-Premises Data Gateway.

---

##  System Architecture & Data Flow

```text
[ Raw CSV Files ] 
       │
       ▼ (Scheduled Ingestion & Cleaning)
[ Apache Airflow (ETL Engine) ]
       │
       ▼ (Loaded via SQLAlchemy / psycopg2 - Admin User)
[ PostgreSQL Data Warehouse ] ── (Managed via) ── [ pgAdmin 4 ]
       │
       ▼ (Restricted via Read-Only "powerbi_user")
[ On-Premises Power BI Gateway ]
       │
       ▼ (Automated Scheduled Refresh)
[ Power BI Service (Cloud Dashboards) ]