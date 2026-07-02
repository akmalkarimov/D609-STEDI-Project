# D609-STEDI-Project
AWS Glue, Athena, Spark, and S3 Data Lakehouse project for the STEDI Human Balance Analytics application.

# STEDI Human Balance Analytics - D609

## Project Overview

This project implements a data lakehouse solution for the STEDI Human Balance Analytics application using AWS.

Technologies used:

- AWS Glue Studio
- Apache Spark (PySpark)
- Amazon Athena
- Amazon S3
- AWS Glue Data Catalog

## ETL Pipeline

The project consists of five AWS Glue jobs:

1. customer_landing_to_trusted
2. accelerometer_landing_to_trusted
3. customers_curated
4. step_trainer_landing_to_trusted
5. machine_learning_curated

## Purpose

The pipeline filters customer data based on privacy consent, joins accelerometer and step trainer data with customer records, and creates curated datasets that can be used for machine learning model training.

## Repository Contents

- customer_landing_to_trusted.py
- accelerometer_landing_to_trusted.py
- customers_curated.py
- step_trainer_landing_to_trusted.py
- machine_learning_curated.py
