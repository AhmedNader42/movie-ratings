from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from helpers import load_csv_data_from

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Movie ratings parser")
    .getOrCreate()
)


movies_bronze = load_csv_data_from(
    path="data/ml-latest-small/movies.csv", session=spark
)

"""
    Columns produced:
        1. year = Extract the year from the movie title
        2. title_clean = Movie title without the year information
        3. genre_list = Split the genres on character "|" and store as a list
"""
movies_silver = (
    movies_bronze.withColumn("year", F.regexp_extract("title", "(\d{4})", 1))
    .withColumn("title_clean", F.regexp_replace("title", "\(\d{4}\)", ""))
    .withColumn("genre_list", F.split("genres", "\|"))
)


""" 
    1. Explode the genre list to get all values.
    2. Keep only distinct genre tags
    3. Generate ID column and assign to each of the unique genres
"""
genres = (
    movies_silver.select(F.explode("genre_list"))
    .distinct()
    .withColumn("id", F.monotonically_increasing_id())
    .withColumnRenamed("col", "genre")
)


"""
    1. Collect the genres with ID pairs as an a local array to the driver node.
    2. broadcast the array to each executor so that it is available locally.
    3. create a map of ID -> GENRE to be able to transform the values.
"""
genres_array = genres.collect()
broadcasted_genres = spark.sparkContext.broadcast(genres_array)
genres_map = F.create_map([F.lit(x) for i in broadcasted_genres.value for x in i])

"""
    1. Add new column with the values transformed using the map above. Replace each value with the corresponding ID
    2. Select only needed columns.
    3. Rename columns
"""
movies_gold = (
    movies_silver.withColumn(
        "transformed_genres", F.transform("genre_list", lambda x: genres_map[x])
    )
    .select("title_clean", "year", "transformed_genres")
    .withColumnRenamed("title_clean", "movie_title")
    .withColumnRenamed("transformed_genres", "genre")
)

"""
    Write partitioned parquet files
"""
movies_gold.write.mode("overwrite").partitionBy("year").parquet("output/movies.parquet")
genres.write.mode("overwrite").partitionBy("genre").parquet("output/genres.parquet")
