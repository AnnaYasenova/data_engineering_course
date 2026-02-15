import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "INPUT_S3_PATH",
    "TARGET_BUCKET"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

INPUT_S3_PATH = args["INPUT_S3_PATH"]
TARGET_BUCKET = args["TARGET_BUCKET"].replace("s3://", "").strip().strip("/")

df = spark.read.json(INPUT_S3_PATH)

silver_df = (df
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("full_name", F.trim(F.col("full_name")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("birth_date", F.to_date(F.col("birth_date")))
    .withColumn("phone_number", F.col("phone_number"))
)

silver_path = f"s3://{TARGET_BUCKET}/silver/user_profiles/"
silver_df.write.mode("overwrite").parquet(silver_path)

job.commit()
