from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import *
from typing import List


class MovieProcessor:
    def __init__(self, *, spark: SparkSession, data: DataFrame):
        self.spark = spark
        self.raw_movies = data
        self.cleaned_movies = self._clean_movies()
        self.movies_with_ratios = self._calculate_revenue_budget_ratio()

    def _ensure_valid_nums(self, key: str):
        pass

    def _calculate_revenue_budget_ratio(self) -> DataFrame:
        pass

    def _clean_movies(self) -> DataFrame:
        return self.raw_movies.withColumn(
            'budget', col('budget').cast(DecimalType(15, 4))
        ).withColumn(
            'revenue',
            col('revenue').cast(DecimalType(15, 4))
        )

    @staticmethod
    def _titles_from_movies(movies: DataFrame) -> List[str]:
        pass

    def top_n_movies(self, n: int) -> DataFrame:
        pass

    def top_n_movie_titles(self, n: int) -> List[str]:
        pass
