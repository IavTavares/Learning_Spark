# to run this file with the respective data file
# 1) Activate the local environment.
# 2) $ .spark_env/bin/spark-submit mnm.py data/mnm_dataset.csv
# $ <folder_path_to_spark>/bin/spark-submit <path_to_source_file> <path_data_file>

import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())

    # to avoid having verbose INFO messages printed to the console
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # get the M&M data set file name from cli
    mnm_file = sys.argv[1]
    # read the file into a Spark DataFrame
    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file))
    mnm_df.show(n=5, truncate=False)

    # aggregate count of all colors and groupBy state and color
    # orderBy descending order
    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # show all the resulting aggregation for all the dates and colors
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # find the aggregate count for California by filtering
    ca_count_mnm_df = (mnm_df.select("*")
                       .where(mnm_df.State == 'CA')
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False))

    # show the resulting aggregation for California
    ca_count_mnm_df.show(n=10, truncate=False)
