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
AWSGlueDataCatalog_node1783011118121 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1783011118121")

# Script generated for node Amazon S3
AmazonS3_node1783096150081 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-akmal-2026-554180008580-us-east-1-an/accelerometer_landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1783096150081")

# Script generated for node SQL Query
SqlQuery2450 = '''
SELECT a.*
FROM accelerometer_landing a
INNER JOIN customer_trusted c
ON a.user = c.email;
'''
SQLQuery_node1783010513393 = sparkSqlQuery(glueContext, query = SqlQuery2450, mapping = {"customer_trusted":AWSGlueDataCatalog_node1783011118121, "accelerometer_landing":AmazonS3_node1783096150081}, transformation_ctx = "SQLQuery_node1783010513393")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783010513393, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783010425864", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783010543738 = glueContext.getSink(path="s3://stedi-akmal-2026-554180008580-us-east-1-an/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1783010543738")
AmazonS3_node1783010543738.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1783010543738.setFormat("glueparquet", compression="snappy")
AmazonS3_node1783010543738.writeFrame(SQLQuery_node1783010513393)
job.commit()
