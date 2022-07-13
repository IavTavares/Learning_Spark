from pyspark.sql import SparkSession

spark = (SparkSession
.builder
.appName("Spark_ML_APP")
.enableHiveSupport() # we need this to create tables
.getOrCreate())

# In Python
filePath = """data/sf-airbnb-clean.parquet/"""
airbnbDF = spark.read.parquet(filePath)

airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
"number_of_reviews", "price").show(5)