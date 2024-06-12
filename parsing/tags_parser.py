from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from helpers import load_csv_data_from

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Movie ratings parser")
    .getOrCreate()
)

tags = load_csv_data_from(path="data/ml-latest-small/tags.csv", session=spark)

tags.show()
