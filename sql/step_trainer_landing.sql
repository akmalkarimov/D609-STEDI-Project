CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
  sensorreadingtime bigint,
  serialnumber string,
  distancefromobject int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-akmal-2026-554180008580-us-east-1-an/step_trainer_landing/';
