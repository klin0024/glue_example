import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import date

args = getResolvedOptions(sys.argv, ["JOB_NAME","OBJECT"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

csvPath = args["OBJECT"]
redshiftConnection = "redshift"
redshiftTmpDir = "s3://aws-glue-assets-435028209039-us-west-2/temporary/"
redshiftDBTable = "public.demo02"
redshiftPreactions = "CREATE TABLE IF NOT EXISTS {redshiftDBTable} (admit VARCHAR, gre VARCHAR, gpa VARCHAR, rank VARCHAR);".format(redshiftDBTable = redshiftDBTable)

print("csvPath: "+ csvPath)


S3bucket = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": [ csvPath ], "recurse": True},
    transformation_ctx="S3bucket",
)


AmazonRedshift = glueContext.write_dynamic_frame.from_options(
    frame=S3bucket,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": redshiftTmpDir,
        "useConnectionProperties": "true",
        "dbtable": redshiftDBTable,
        "connectionName": redshiftConnection,
        "preactions": redshiftPreactions,
    },
    transformation_ctx="AmazonRedshift",
)

job.commit()
