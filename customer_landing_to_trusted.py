import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://savar-udacity-nd/project/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node only_research_allowing_customers
only_research_allowing_customers_node1690273095584 = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="only_research_allowing_customers_node1690273095584",
)

# Script generated for node trusted_customer
trusted_customer_node3 = glueContext.write_dynamic_frame.from_options(
    frame=only_research_allowing_customers_node1690273095584,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://savar-udacity-nd/project/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="trusted_customer_node3",
)

job.commit()
