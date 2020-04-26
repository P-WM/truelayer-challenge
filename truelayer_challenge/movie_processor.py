from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import *
from typing import List


class MovieProcessor:
    def __init__(self, *, spark: SparkSession, data: DataFrame):
        self.spark = spark
        self.raw_movies = data

    def _ensure_valid_nums(self, key: str):
        pass

    @staticmethod
    def _calculate_revenue_budget_ratio(movies: DataFrame) -> DataFrame:
        return movies.withColumn( \
            'revenue_budget_ratio',
            (movies.revenue / movies.budget).cast(DecimalType(7, 2))
        )

    @staticmethod
    def _clean_movies(movies: DataFrame) -> DataFrame:
        return movies.withColumn('budget', col('budget').cast(DecimalType(15, 4))) \
            .withColumn('revenue', col('revenue').cast(DecimalType(15, 4))) \
            .where(col('budget') >= 1000) \
            .where(col('revenue') >= 1000)

    @staticmethod
    def _titles_from_movies(movies: DataFrame) -> List[str]:
        pass

    @property
    def all_movies(self) -> DataFrame:
        return self.raw_movies

    def top_n_movies(self, n: int) -> DataFrame:
        pass

    def top_n_movie_titles(self, n: int) -> List[str]:
        pass
