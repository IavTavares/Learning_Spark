from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc, when

# Create a SparkSession
spark = (SparkSession
.builder
.appName("SparkSQLExampleApp")
.enableHiveSupport() # we need this to create tables
.getOrCreate())

# Path to data set
csv_file = "./data/departuredelays.csv"
# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)
df = (spark.read.format("csv")
.option("inferSchema", "true")
.option("header", "true")
.load(csv_file))

df.createOrReplaceTempView("us_delay_flights_tbl")

# spark.sql("""SELECT distance, origin, destination
# FROM us_delay_flights_tbl WHERE distance > 1000
# ORDER BY distance DESC""").show(10)

# # same as query above, but with a spark API flavour
# (df.select("distance", "origin", "destination")
# .where(col("distance") > 1000)
# .orderBy(desc("distance"))).show(10)

# spark.sql("""SELECT date, delay, origin, destination
# FROM us_delay_flights_tbl
# WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
# ORDER by delay DESC""").show(10)
# # same as query above, but with a spark API flavour
# (df.select("date", "delay", "origin","destination")
# .where((col("delay") > 120) & (col("ORIGIN") == "SFO") & 
#             (col("DESTINATION")== "ORD"))
# .orderBy(desc("delay"))).show(10)

# spark.sql("""SELECT delay, origin, destination,
# CASE
# WHEN delay > 360 THEN 'Very Long Delays'
# WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
# WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
# WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
# WHEN delay = 0 THEN 'No Delays'
# ELSE 'Early'
# END AS Flight_Delays
# FROM us_delay_flights_tbl
# ORDER BY origin, delay DESC""").show(10)
# same as query above, but with a spark API flavour
(df.select("delay","origin",col("destination"),
    when(df.delay > 360,"Very Long Delays")
    .when((df.delay > 120) & (df.delay < 360),"Long Delays")
    .when((df.delay > 60) & (df.delay < 120),"Short Delays")
    .when((df.delay > 0) & (df.delay < 60),"Tolerable Delays")
    .when((df.delay == 0),"No Delays")
    .otherwise("Early").alias("Flight_Delays")
    )
    .orderBy(asc("ORIGIN"),desc("delay"))
).show(10)

# spark.sql("CREATE DATABASE learn_spark_db") # run this only once
# spark.sql("USE learn_spark_db") 
# from now onwards, every table is created in learn_spark_db

# run below only once
# spark.sql("""CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT,
# distance INT, origin STRING, destination STRING)""")

# Saving df as SQL Table. It'll be saved in learn_spark_db
# (df.write
# .mode("overwrite")
# .saveAsTable("us_delay_flights_tbl"))

#Saving df as json file
(df.write.format("json")
.mode("overwrite")
#.option("compression", "snappy")
.save("./data/df_json"))
