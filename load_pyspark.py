from pyspark.sql import SparkSession
from pymongo import MongoClient

client = MongoClient("mongodb+srv://farhanandri:farhanandri@fandris.xhdfdwt.mongodb.net/")

spark = SparkSession.builder \
    .appName("VGSales_Load").getOrCreate()

df = spark.read.csv('/opt/airflow/data/transform/', header=True, inferSchema=True)

list_of_rows = df.collect()

document_list = [row.asDict() for row in list_of_rows]

client['video_game_store']['video_game_sales'].insert_many(document_list)

spark.stop()