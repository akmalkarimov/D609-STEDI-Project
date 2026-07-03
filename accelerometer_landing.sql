CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    user string,
    timestamp bigint,
    x double,
    y double,
    z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-akmal-2026-554180008580-us-east-1-an/accelerometer_landing/';
