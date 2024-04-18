from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = (
    SparkSession.builder.master("local[*]")
    .appName("Repartition and Coalesce")
    .getOrCreate()
)

movies = spark.read.option("header", True).csv("data/ml-latest-small/movies.csv")
movies_year = movies.withColumn("year", F.regexp_extract("title", "(\d{4})", 1))
movies_grouped_By_Year = movies_year.groupBy("year").count().orderBy("year")
movies_grouped_By_Year.show()
