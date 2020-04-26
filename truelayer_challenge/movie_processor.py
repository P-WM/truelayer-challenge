from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col
from typing import List


class MovieProcessor:
    def __init__(self, *, spark: SparkSession, data: DataFrame):
        self.spark = spark
        self.raw_movies = data

    @staticmethod
    def _ensure_valid_money(movies: DataFrame, key: str):
        return movies \
            .withColumn(key, col(key).cast(DecimalType(15, 4))) \
            .where(col(key) >= 1_000)

    @classmethod
    def _clean_movies(cls, movies: DataFrame) -> DataFrame:
        """
        Currently this does nothing but cast money to a safe type and
        remove any movie with a budget or revenue below $1,000USD.
        """
        safe_budget = cls._ensure_valid_money(movies, 'budget')
        safe_revenue = cls._ensure_valid_money(safe_budget, 'revenue')

        return safe_revenue

    @staticmethod
    def _calculate_revenue_budget_ratio(movies: DataFrame) -> DataFrame:
        return movies.withColumn( \
            'revenue_budget_ratio',
            (movies.revenue / movies.budget).cast(DecimalType(7, 2))
        )

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
