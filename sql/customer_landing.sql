CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
  serialnumber string,
  sharewithpublicasofdate bigint,
  birthday string,
  registrationdate bigint,
  sharewithresearchasofdate bigint,
  customername string,
  email string,
  lastupdatedate bigint,
  phone string,
  sharewithfriendsasofdate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-akmal-2026-554180008580-us-east-1-an/customer_landing/';
