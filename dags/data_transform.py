from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StringType, IntegerType, FloatType

print("Spark is starting")

spark = SparkSession.builder \
    .appName("Minio_to_Postgres") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "dataopsadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "dataopsadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("reading from minio")

try:
    df1 = spark.read.csv("s3a://dataops-bronze/raw/dirty_store_transactions.csv", header=True, inferSchema=True)
    print("read data successfuly")
except:
    print("reading data error")
    raise

df2 = df1.withColumnRenamed("Date", "Date_Casted")
df2 = df2.withColumn("STORE_ID", F.trim(F.regexp_replace(F.col("STORE_ID"), "[^A-Za-z0-9]", "")))

columns_letter = ["STORE_LOCATION", "PRODUCT_CATEGORY"]
for col_let in columns_letter:
    df2 = df2.withColumn(col_let, F.trim(F.regexp_replace(F.col(col_let), "[^A-Za-z ]", "")))
columns_digit = ["PRODUCT_ID", "MRP", "CP","DISCOUNT", "SP"]

for col_dig in columns_digit:
    df2 = df2.withColumn(col_dig, F.trim(F.regexp_replace(F.col(col_dig), "[^0-9.]", "")))

change_to_string = ["STORE_ID", "STORE_LOCATION", "PRODUCT_CATEGORY"]

for ch_str in change_to_string:
    df2 = df2.withColumn(ch_str, F.col(ch_str).cast(StringType()))

df2 = df2.withColumn("PRODUCT_ID", F.col("PRODUCT_ID").cast(IntegerType()))

change_to_float = ["MRP", "CP", "DISCOUNT", "SP"]

for ch_flo in change_to_float:
    df2 = df2.withColumn(ch_flo, F.col(ch_flo).cast(FloatType()))

df2 = df2.withColumn("Date_Casted", F.to_date(F.trim(F.col("Date_Casted")), "yyyy-M-d"))

print("write to postgres")
try:
    df2.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/traindb") \
        .option("dbtable", "clean_data_transactions") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print("completed")
except:
    print("writing postgres error")
    raise

spark.stop()