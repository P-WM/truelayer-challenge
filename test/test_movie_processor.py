from unittest import TestCase
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StructField, DecimalType

from truelayer_challenge.movie_processor import MovieProcessor
from test.fixtures import movies


class TestMovieProcess(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName(
            "TestMovieProcessor").getOrCreate()

    def test_cleans_number_columns_correctly(self):
        pass

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
