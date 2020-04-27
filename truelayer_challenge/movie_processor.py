from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType, DateType, ArrayType, StructField, StructType, StringType
from pyspark.sql.functions import col, from_json, year
from typing import List


class MovieProcessor:
    def __init__(self, *, data: DataFrame):
        self.raw_movies = data
        self.processed_movies = None

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

        return safe_revenue \
            .withColumn('rating', col('popularity').cast(DecimalType(10, 6))) \
            .withColumn('release_date', col('release_date').cast(DateType()))

    @staticmethod
    def _calculate_revenue_budget_ratio(movies: DataFrame) -> DataFrame:
        return movies.withColumn( \
            'revenue_budget_ratio',
            (movies.revenue / movies.budget).cast(DecimalType(8, 2))
        )

    @staticmethod
    def _add_production_company_names(movies: DataFrame) -> DataFrame:
        production_company_schema = ArrayType(
            StructType([StructField('name', StringType(), False)]))

        return movies \
            .withColumn('production_companies', from_json(movies.production_companies, production_company_schema)) \
            .withColumn('production_companies', col('production_companies.name'))

    @staticmethod
    def _add_year(movies: DataFrame) -> DataFrame:
        return movies \
            .withColumn('year', year(movies.release_date))

    @staticmethod
    def _titles_from_movies(movies: DataFrame) -> List[str]:
        return [movie.title for movie in movies.collect()]

    @property
    def all_movies(self) -> DataFrame:
        if self.processed_movies:
            return self.processed_movies

        processed_movies = self.raw_movies

        process_steps = [
            self._clean_movies, self._calculate_revenue_budget_ratio,
            self._add_year, self._add_production_company_names
        ]

        for step in process_steps:
            processed_movies = step(processed_movies)

        self.processed_movies = processed_movies

        return processed_movies.select('title', 'production_companies',
                                       'release_date', 'rating',
                                       'revenue_budget_ratio', 'budget',
                                       'revenue', 'year')

    def top_n_movies(self, n: int) -> DataFrame:
        return self.all_movies \
            .orderBy(col('revenue_budget_ratio'), ascending=False) \
            .limit(n) \
            .cache()

    def top_n_movie_titles(self, n: int) -> List[str]:
        top_n_movies = self.top_n_movies(n)

        return self._titles_from_movies(top_n_movies)
