# Athena Results

This folder contains screenshots of Amazon Athena query results used to validate the data pipeline for the STEDI Human Balance Analytics project.

Included screenshots verify:

## Landing Zone
- customer_landing row count
- accelerometer_landing row count
- step_trainer_landing row count
- customer_landing records with blank shareWithResearchAsOfDate

## Trusted Zone
- customer_trusted row count
- customer_trusted contains no blank shareWithResearchAsOfDate values
- accelerometer_trusted row count
- step_trainer_trusted row count

## Curated Zone
- customers_curated row count
- machine_learning_curated row count

These screenshots serve as supporting evidence that the ETL pipeline produced the expected outputs in Amazon Athena.
