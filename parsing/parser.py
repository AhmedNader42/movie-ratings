from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Repartition and Coalesce")
    .getOrCreate()
)

movies = spark.read.option("header", True).csv("data/ml-latest-small/movies.csv")

movies.show(5)
