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

# Script generated for node customer_trusted
customer_trusted_node1690276430025 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://savar-udacity-nd/project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1690276430025",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://savar-udacity-nd/project/ Accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1",
)

# Script generated for node customer_privacy_filter
customer_privacy_filter_node1690276783401 = Join.apply(
    frame1=accelerometer_landing_node1,
    frame2=customer_trusted_node1690276430025,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="customer_privacy_filter_node1690276783401",
)

# Script generated for node Drop Fields
DropFields_node1690276988378 = DropFields.apply(
    frame=customer_privacy_filter_node1690276783401,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1690276988378",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1690276988378,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://savar-udacity-nd/project/ Accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
