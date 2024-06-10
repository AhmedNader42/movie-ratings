from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = (
    SparkSession.builder.master("local[*]")
    .appName("Repartition and Coalesce")
    .getOrCreate()
)

movies = spark.read.option("header", True).csv("data/ml-latest-small/movies.csv")
# Extract the year from the movie title
movies_with_year = movies.withColumn("year", F.regexp_extract("title", "(\d{4})", 1))

# Extract the genre tags from genres column
movies_genre_separated = movies_with_year.rdd.flatMap(lambda x: x.genres.split("|"))


print(movies_genre_separated.collect())


# movies_grouped_By_Year = movies_with_year.groupBy("year").count().orderBy("year")
# movies_grouped_By_Year.show()
