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

# # Extract the year from the movie title
# movies_with_year = movies.withColumn("year", F.regexp_extract("title", "(\d{4})", 1))

# # Extract the genre tags from genres column
# movies_genre_separated = movies_with_year.withColumn(
#     "genre_list", F.split("genres", "\|")
# )

# # Get the distinct genre tags
# genres = movies_genre_separated.select(F.explode("genre_list")).distinct()

# # Assign IDs for each genre
# genres_With_ID = genres.withColumn("id", F.monotonically_increasing_id())

# genres_With_ID.show(1000, False)
