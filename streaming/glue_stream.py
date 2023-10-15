import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

kinesisStreamARN = "arn:aws:kinesis:us-west-2:435028209039:stream/glue"
kinesisClassification = "json"
kinesisWindowSize = "60 seconds"
redshiftConnection = "redshift"
redshiftTmpDir = "s3://aws-glue-assets-435028209039-us-west-2/temporary/"
redshiftDBTable = "public.demo03"
redshiftPreactions = "CREATE TABLE IF NOT EXISTS {redshiftDBTable} (admit VARCHAR, gre VARCHAR, gpa VARCHAR, rank VARCHAR);".format(redshiftDBTable = redshiftDBTable)


dataframe_KinesisStream = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": kinesisStreamARN,
        "classification": kinesisClassification,
        "startingPosition": "latest",
        "inferSchema": "true",
    },
    transformation_ctx="dataframe_KinesisStream",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        KinesisStream = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )

        AmazonRedshift = glueContext.write_dynamic_frame.from_options(
            frame=KinesisStream,
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


glueContext.forEachBatch(
    frame=dataframe_KinesisStream,
    batch_function=processBatch,
    options={
        "windowSize": kinesisWindowSize,
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
