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

# Script generated for node Amazon S3
AmazonS3_node1783095427062 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-akmal-2026-554180008580-us-east-1-an/customer_landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1783095427062")

# Script generated for node SQL Query
SqlQuery2442 = '''
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL;
'''
SQLQuery_node1783005963583 = sparkSqlQuery(glueContext, query = SqlQuery2442, mapping = {"customer_landing":AmazonS3_node1783095427062}, transformation_ctx = "SQLQuery_node1783005963583")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783005963583, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783005760701", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783006746582 = glueContext.getSink(path="s3://stedi-akmal-2026-554180008580-us-east-1-an/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1783006746582")
AmazonS3_node1783006746582.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1783006746582.setFormat("glueparquet", compression="snappy")
AmazonS3_node1783006746582.writeFrame(SQLQuery_node1783005963583)
job.commit()