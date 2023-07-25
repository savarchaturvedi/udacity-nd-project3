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

# Script generated for node customer_curated_data
customer_curated_data_node1690301234945 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://savar-udacity-nd/project/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_data_node1690301234945",
)

# Script generated for node step_trainer_data
step_trainer_data_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://savar-udacity-nd/project/Step Trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_data_node1",
)

# Script generated for node Join
Join_node1690301223249 = Join.apply(
    frame1=step_trainer_data_node1,
    frame2=customer_curated_data_node1690301234945,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1690301223249",
)

# Script generated for node Drop Fields
DropFields_node1690301453712 = DropFields.apply(
    frame=Join_node1690301223249,
    paths=[
        "`.serialNumber`",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1690301453712",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1690301453712,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://savar-udacity-nd/project/Step Trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
