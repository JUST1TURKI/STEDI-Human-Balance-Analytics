import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get the job name from command line arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize SparkContext, GlueContext, and Glue job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load data from Accelerometer Landing in JSON format
accelerometer_landing_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-s3/accelerometer/landing/"],
        "recurse": True,
        "enableUpdateCatalog": True
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Load data from Customer Trusted in JSON format
customer_trusted_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-s3/customer/trusted/"], "recurse": True,"enableUpdateCatalog": True},
    transformation_ctx="CustomerTrusted_node1693315552442",
)

# Join Accelerometer Landing and Customer Trusted frames on 'user' and 'email' keys
joined_frame = Join.apply(
    frame1=accelerometer_landing_frame,
    frame2=customer_trusted_frame,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1693315680778",
)

# Apply field mapping and drop unnecessary fields
mapped_frame = ApplyMapping.apply(
    frame=joined_frame,
    mappings=[
        ("user", "string", "user", "string"),
        ("timeStamp", "long", "timeStamp", "long"),
        ("x", "double", "x", "float"),
        ("y", "double", "y", "float"),
        ("z", "double", "z", "float"),
    ],
    transformation_ctx="DropFields_node1693315932531",
)

# Write the resulting frame to Accelerometer Trusted in JSON format
glueContext.write_dynamic_frame.from_options(
    frame=mapped_frame,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-s3/accelerometer/trusted/",
        "partitionKeys": [],
        "enableUpdateCatalog": True
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

# Commit the Glue job
job.commit()
