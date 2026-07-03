
# Landing Zone SQL DDL Scripts

This folder contains the SQL Data Definition Language (DDL) scripts used to manually create the Landing Zone tables in Amazon Athena for the STEDI Human Balance Analytics project.

## Included Scripts

### customer_landing.sql
Creates the `customer_landing` table from the customer JSON dataset using appropriate data types and the correct Amazon S3 landing location.

### accelerometer_landing.sql
Creates the `accelerometer_landing` table from the accelerometer JSON dataset using appropriate data types and the correct Amazon S3 landing location.

### step_trainer_landing.sql
Creates the `step_trainer_landing` table from the step trainer JSON dataset using appropriate data types and the correct Amazon S3 landing location.

Each script includes:

- CREATE EXTERNAL TABLE statement
- Complete JSON schema
- Appropriate data types
- JSON SerDe configuration
- Amazon S3 LOCATION for the corresponding Landing Zone dataset

These scripts were used to manually create the Landing Zone tables before processing the data through the Trusted and Curated layers.
