import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783014890416 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1783014890416")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783014892480 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1783014892480")

# Script generated for node SQL Query
SqlQuery2238 = '''
SELECT
  s.sensorreadingtime,
  s.serialnumber,
  s.distancefromobject,
  a.user,
  a.x,
  a.y,
  a.z
FROM step_trainer_trusted s
INNER JOIN accelerometer_trusted a
ON s.sensorreadingtime = a.timestamp;
'''
SQLQuery_node1783014926906 = sparkSqlQuery(glueContext, query = SqlQuery2238, mapping = {"accelerometer_trusted":AWSGlueDataCatalog_node1783014890416, "step_trainer_trusted":AWSGlueDataCatalog_node1783014892480}, transformation_ctx = "SQLQuery_node1783014926906")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783014926906, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783014863824", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783014983410 = glueContext.getSink(path="s3://stedi-akmal-2026-554180008580-us-east-1-an/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1783014983410")
AmazonS3_node1783014983410.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1783014983410.setFormat("glueparquet", compression="snappy")
AmazonS3_node1783014983410.writeFrame(SQLQuery_node1783014926906)
job.commit()