AWS Glue ETL Python scripts for the STEDI Human Balance Analytics project.

# AWS Glue ETL Python Scripts

This folder contains the AWS Glue ETL scripts used for the STEDI Human Balance Analytics project.

## Landing to Trusted

### customer_landing_to_trusted.py
Reads customer data directly from the S3 Landing Zone, filters records where `shareWithResearchAsOfDate` is not null, and writes the approved records to the `customer_trusted` table.

### accelerometer_landing_to_trusted.py
Reads accelerometer data directly from the S3 Landing Zone, joins it with the `customer_trusted` table using the customer email address, and writes the resulting accelerometer records to the `accelerometer_trusted` table.

### step_trainer_landing_to_trusted.py
Reads step trainer data directly from the S3 Landing Zone, joins it with the `customers_curated` table using the serial number, and writes the results to the `step_trainer_trusted` table.

## Curated Layer

### customers_curated.py
Joins `customer_trusted` with `accelerometer_trusted` using the customer email address and produces the `customers_curated` dataset.

### machine_learning_curated.py
Joins `step_trainer_trusted` with `accelerometer_trusted` using the sensor reading timestamp to create the final `machine_learning_curated` dataset used for machine learning.

All scripts were generated and configured using AWS Glue Studio and implement the ETL pipeline required for the STEDI Human Balance Analytics project.
