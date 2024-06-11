from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = (
    SparkSession.builder.master("local[*]")
    .appName("Movie ratings parser")
    .getOrCreate()
)

movies = spark.read.option("header", True).csv("data/ml-latest-small/movies.csv")
# Extract the year from the movie title
movies_with_year = movies.withColumn("year", F.regexp_extract("title", "(\d{4})", 1))

# Extract the genre tags from genres column
movies_genre_separated = movies_with_year.withColumn(
    "genre_split", F.split("genres", "\|")
)

movies_genre_separated.show()


# movies_grouped_By_Year = movies_with_year.groupBy("year").count().orderBy("year")
# movies_grouped_By_Year.show()
