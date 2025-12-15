from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window 

spark = SparkSession.builder \
    .appName("VGSalesTransform") \
    .getOrCreate()

# Read Data
df = spark.read.csv(
    path='/opt/airflow/data/raw/', 
    header=True,
    inferSchema=True,
    nullValue="N/A"
)

# Menghapus missing kolom year
df = df.dropna(subset=['Year'])
# Mengisi Unknown di kolom publisher 
df = df.fillna("Unknown", subset=['Publisher']) 

# Mengurutkan row berdasarkan global sales untuk rank
df = df.orderBy(F.col("Global_Sales").desc()) 

# Mereset kolom rank dan mengisi ulang berdasarkan urutan global_sales
df = df.withColumn(
    "Rank", 
    F.row_number().over(
        Window.orderBy(F.col("Global_Sales").desc())
    )
)

# Mengganti tipe data Year menjadi Int
df = df.withColumn("Year", F.col("Year").cast(IntegerType()))

# Write CSV
df.coalesce(1).write.mode("overwrite").csv(
    '/opt/airflow/data/transform/', 
    header=True
)

# Stop Spark
spark.stop()