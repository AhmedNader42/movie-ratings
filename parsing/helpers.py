from pyspark.sql import SparkSession, DataFrame


def load_csv_data_from(path: str, session: SparkSession) -> DataFrame:
    if path and session:
        return session.read.option("header", True).option("inferSchema", True).csv(path)
    raise FileNotFoundError
