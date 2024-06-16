from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from helpers import load_csv_data_from

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Movie ratings parser")
    .getOrCreate()
)


movies = load_csv_data_from(path="data/ml-latest-small/movies.csv", session=spark)

movies.show()

"""
    Columns produced:
        1. year = Extract the year from the movie title
        2. title_clean = Movie title without the year information
        3. genre_list = Split the genres on character "|" and store as a list
"""
movies_with_year = (
    movies.withColumn("year", F.regexp_extract("title", "(\d{4})", 1))
    .withColumn("title_clean", F.regexp_replace("title", "\(\d{4}\)", ""))
    .withColumn("genre_list", F.split("genres", "\|"))
)


""" 
    1. Explode the genre list to get all values.
    2. Keep only distinct genre tags
    3. Generate ID column and assign to each of the unique genres
"""
genres = (
    movies_with_year.select(F.explode("genre_list"))
    .distinct()
    .withColumn("id", F.monotonically_increasing_id())
)

genres_map = F.create_map([F.lit(x) for x in genres.foreach()])

print(genres_map)


# genres.write.mode("overwrite").parquet("output/genres.parquet")

# genres_With_ID.show(1000, False)
