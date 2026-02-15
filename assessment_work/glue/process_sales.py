import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "DATABASE",
    "SOURCE_TABLE",
    "TARGET_BUCKET"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DATABASE = args["DATABASE"]
SOURCE_TABLE = args["SOURCE_TABLE"]
TARGET_BUCKET = args["TARGET_BUCKET"]

# -------- RAW → BRONZE --------
raw_df = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=SOURCE_TABLE
).toDF()

bronze_df = raw_df.select(
    F.col("CustomerId").cast("string"),
    F.col("PurchaseDate").cast("string"),
    F.col("Product").cast("string"),
    F.col("Price").cast("string")
)

bronze_path = f"s3://{TARGET_BUCKET}/bronze/sales/"
bronze_df.write.mode("overwrite").parquet(bronze_path)

# -------- BRONZE → SILVER --------
silver_df = bronze_df \
    .withColumn("client_id", F.col("CustomerId").cast("int")) \
    .withColumn("purchase_date", F.to_date(F.col("PurchaseDate"))) \
    .withColumn("product_name", F.trim("Product")) \
    .withColumn("price",
        F.regexp_replace("Price", r"[^0-9\.\,]", "")
    ) \
    .withColumn("price",
        F.regexp_replace("price", ",", ".").cast(DoubleType())
    ) \
    .select("client_id", "purchase_date", "product_name", "price") \
    .filter(F.col("client_id").isNotNull())

silver_path = f"s3://{TARGET_BUCKET}/silver/sales/"

silver_df.write \
    .mode("overwrite") \
    .partitionBy("purchase_date") \
    .parquet(silver_path)

job.commit()
