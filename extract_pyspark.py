from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("VGSalesExtrack").getOrCreate()


df = spark.read.csv(
    path='/opt/airflow/data/vgsales_raw.csv',
    header=True, 
    inferSchema=True
)

df.coalesce(1).write.csv(
    path='/opt/airflow/data/raw',
    mode='overwrite',
    header=True)

spark.stop()