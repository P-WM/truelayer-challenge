from pyspark.sql import SparkSession, DataFrame
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
        pass

    @staticmethod
    def _titles_from_movies(movies: DataFrame) -> List[str]:
        pass

    def top_n_movies(self, n: int) -> DataFrame:
        pass

    def top_n_movie_titles(self, n: int) -> List[str]:
        pass
