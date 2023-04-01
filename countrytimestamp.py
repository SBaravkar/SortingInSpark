from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    # create a SparkSession object
    spark = SparkSession.builder.appName("SortByCountryTimestamp").getOrCreate()

    # read input CSV file
    input_file_path = sys.argv[1]
    input_file = spark.read.option("header", "true").csv(input_file_path)

    # sort the data by the country and then by the timestamp
    output_file = input_file.orderBy(col("cca2"), col("timestamp"))

    # write the sorted output to CSV file
    output_file_path = sys.argv[2]
    output_file.write.option("header", "true").csv(output_file_path)

    # stop the SparkSession object
    spark.stop()

