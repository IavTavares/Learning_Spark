import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, expr, count_distinct,to_timestamp,year

spark = (SparkSession
        .builder
        .appName("Python_SF_Fires")
        .getOrCreate())
# to avoid having verbose INFO messages printed to the console
sc = spark.sparkContext
sc.setLogLevel("WARN")

sf_fire_filepath = """data/sf-fire-calls.csv"""

# inferring schema from a small sample
sample_df = spark.read.option("samplingRation",0.001)\
            .option("header",True).csv(sf_fire_filepath)
sample_df.printSchema()
sample_df.show(10)

fire_schema = sample_df.schema# .json() # also capable of returning in json format
print(fire_schema)


# get the sf-fire data from reading directly the file
sf_fire_df = spark.read.csv(sf_fire_filepath, header = True, schema= fire_schema)

# showing the 1st five rows of 3 columns such that CallType != Medical Incident
few_fire_df = sf_fire_df.select("IncidentNumber","AvailableDtTm","CallType")\
                        .where(col("CallType") != "Medical Incident")
few_fire_df.show(n=5,truncate=False)

# seeing all the distinct values of CallType column, and counting them
sf_fire_df.select("CallType").where(col("CallType")\
    .isNotNull()).groupby("CallType").count().show(truncate= False)
# probably not the simplest way of seeing the distinct values of a column.

# simpler way of seeing all the distinct values of CallType column
sf_fire_df.select("CallType").where(col("CallType").isNotNull())\
        .distinct().orderBy("CallType", ascending= True).show(truncate= False)
# orderBy was optional


# counting distinct CallTypes != Null
sf_fire_df.select("CallType").where(col("CallType").isNotNull()).agg(count_distinct("CallType"))\
            .alias("DistinctCallTypes").show()

# renaming a column
new_fire_df = sf_fire_df.withColumnRenamed("Delay","ResponseDelayedinMins")
new_fire_df.select("CallType","ResponseDelayedinMins")\
    .where(col("ResponseDelayedinMins")>5).show(truncate = False)

fire_ts_df = new_fire_df.withColumn("IncidentDate",
                            to_timestamp(col("CallDate"),"MM/dd/yyyy"))\
                        .drop("CallDate")\
                        .withColumn("AvailableDtTs",
                            to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a"))\
                        .drop("AvailableDtTm")
# IncidentDate has hours and full dates. We're just selecting according to year
fire_ts_df.select(year("IncidentDate")).distinct()\
            .orderBy(year("IncidentDate")).show()

# We're selecting all, grouping by year of the indident, and sorting from greater to smaller
fire_ts_df.select("*").groupBy(year("IncidentDate")).count()\
            .orderBy("count",ascending=False).show(truncate=False)
