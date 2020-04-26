from unittest import TestCase
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DecimalType

from truelayer_challenge.movie_processor import MovieProcessor
from test.fixtures import movies, movies_with_low_bud_or_rev


class TestMovieProcess(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName(
            "TestMovieProcessor").getOrCreate()

    def compare_unordered_dataframes(self, actual: DataFrame, expected: DataFrame):
        actual_for_comparison = set(actual.collect())
        expected_for_comparison = set(expected.collect())
        print(actual_for_comparison)
        self.assertEqual(actual_for_comparison, expected_for_comparison)


    def test_cleans_number_columns_correctly(self):
        """
        Some have budgets and/or revenues of 0. Some have wildly unrealistic numbers.
        Some may have accurate numbers but have been made extremely cheaply or
        have been extraordinarily unsuccessful 🤷‍♀️

        The impossible or inaccurate numbers introduce the possibility of division by zero
        or simply inaccurate results. I've decided here to go for the simple (and rather arbitrary)
        solution of excluding any movie with a budget or revenue of less than $1,000USD.

        Obviously in practice we'd consider a more useful heuristic perhaps taking into 
        account the year and country in which the film was made etc. After all, we're looking
        for accurate outliers: we want to know if a film did surprisingly well despite an infinitesimal
        budget (and vice-versa 😬)
        """
        test_movies = self.spark.createDataFrame(movies + movies_with_low_bud_or_rev)
        test_processor = MovieProcessor(spark=self.spark, data=test_movies)

        actual_movies = test_processor._clean_movies().select('title', 'id')
        expected_movies = self.spark.createDataFrame(movies).select('title', 'id')

        self.compare_unordered_dataframes(actual=actual_movies, expected=expected_movies)

    def test_treats_money_with_appropriate_precision(self):
        test_movies = self.spark.createDataFrame(movies)
        test_processor = MovieProcessor(spark=self.spark, data=test_movies)

        actual_schema = test_processor._clean_movies().select(
            'budget', 'revenue').schema

        expected_schema = StructType([
            StructField('budget', DecimalType(15, 4), True),
            StructField('revenue', DecimalType(15, 4), True),
        ])

        self.assertEqual(actual_schema, expected_schema)

    def parses_year_correctly(self):
        pass

    def calculates_ratios_correctly(self):
        pass
