import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://savar-udacity-nd/project/ Accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1",
)

# Script generated for node step_trainer_data
step_trainer_data_node1690306878705 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://savar-udacity-nd/project/Step Trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_data_node1690306878705",
)

# Script generated for node Join
Join_node1690306882905 = Join.apply(
    frame1=accelerometer_trusted_node1,
    frame2=step_trainer_data_node1690306878705,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1690306882905",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1690306882905,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://savar-udacity-nd/project/", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

job.commit()
