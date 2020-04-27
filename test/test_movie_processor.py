from unittest import TestCase
from decimal import Decimal
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, DecimalType, DateType, StringType

from truelayer_challenge.movie_processor import MovieProcessor
from test.fixtures import movies, movies_with_low_bud_or_rev


class TestMovieProcess(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName(
            "TestMovieProcessor").getOrCreate()

        cls.test_movies = cls.spark.createDataFrame(movies)

    def setUp(self):
        self.test_processor = MovieProcessor(data=self.test_movies)

    def compare_unordered_dataframes(self, actual: DataFrame,
                                     expected: DataFrame):
        actual_for_comparison = actual.collect()
        expected_for_comparison = expected.collect()

        self.assertCountEqual(actual_for_comparison, expected_for_comparison)

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
        test_movies = self.spark.createDataFrame(movies +
                                                 movies_with_low_bud_or_rev)
        test_processor = MovieProcessor(data=test_movies)

        actual_movies = test_processor._clean_movies(test_movies).select(
            'title', 'id')
        expected_movies = self.spark.createDataFrame(movies).select(
            'title', 'id')

        self.compare_unordered_dataframes(actual=actual_movies,
                                          expected=expected_movies)

    def test_treats_money_with_appropriate_precision(self):
        actual_schema = self.test_processor._clean_movies(
            self.test_movies).select('budget', 'revenue').schema

        expected_schema = StructType([
            StructField('budget', DecimalType(15, 4), True),
            StructField('revenue', DecimalType(15, 4), True),
        ])

        self.assertEqual(actual_schema, expected_schema)

    def test_concats_production_company_names(self):
        actual_movies = self.test_processor._add_production_company_names(
            self.test_movies).select('production_companies',
                                     'title').collect()

        expected_movies = [
            Row(title='Executive Decision',
                production_companies=['Silver Pictures', 'Warner Bros.']),
            Row(title='Mission: Impossible II',
                production_companies=[
                    'Paramount Pictures', 'Cruise/Wagner Productions',
                    'Munich Film Partners & Company (MFP) MI2 Productions'
                ]),
            Row(title='Shalako',
                production_companies=[
                    'Central Cinema Company Film',
                    'Palomar Pictures International',
                    'Kingston Film Productions Ltd.'
                ]),
            Row(title='Anywhere But Here',
                production_companies=[
                    'Twentieth Century Fox Film Corporation',
                    'Fox 2000 Pictures'
                ]),
            Row(title='The Strangers',
                production_companies=[
                    'Rogue Pictures', 'Vertigo Entertainment',
                    'Intrepid Pictures'
                ])
        ]

        self.assertCountEqual(actual_movies, expected_movies)

    def test_returns_correct_schema(self):
        actual_schema = self.test_processor.all_movies.schema
        expected_schema = StructType([
            StructField('title', StringType(), True),
            StructField('production_companies', ArrayType(StringType(), True),
                        True),
            StructField('release_date', DateType(), True),
            StructField('rating', DecimalType(10, 6), True),
            StructField('revenue_budget_ratio', DecimalType(8, 2), True),
            StructField('budget', DecimalType(15, 4), True),
            StructField('revenue', DecimalType(15, 4), True),
            StructField('year', IntegerType(), True),
            StructField('movie_id', StringType(), False)
        ])

        self.assertEqual(actual_schema, expected_schema)

    def test_adds_the_correct_year(self):
        clean_movies = self.test_processor._clean_movies(self.test_movies)
        actual_movies = self.test_processor._add_year(clean_movies).select(
            'title', 'year').collect()
        expected_movies = [
            Row(title='Executive Decision', year=1996),
            Row(title='Mission: Impossible II', year=2000),
            Row(title='Shalako', year=1968),
            Row(title='Anywhere But Here', year=1999),
            Row(title='The Strangers', year=2008)
        ]

        self.assertCountEqual(actual_movies, expected_movies)

    def test_adds_the_correct_movie_id(self):
        """
        This gives us something unique(ish) but deterministic to join on as movie titles are likely to collide
        """
        clean_movies = self.test_processor._clean_movies(self.test_movies)
        actual_movies = self.test_processor._add_movie_id(clean_movies).select(
            'movie_id', 'title').collect()
        expected_movies = [
            Row(title='Executive Decision', movie_id='FFFFFFFFBF6AD93F'),
            Row(title='Mission: Impossible II', movie_id='25E22182'),
            Row(title='Shalako', movie_id='FFFFFFFFB28C5FBD'),
            Row(title='Anywhere But Here', movie_id='752E4DDB'),
            Row(title='The Strangers', movie_id='46A353C9')
        ]

        self.assertCountEqual(actual_movies, expected_movies)

    def test_calculates_ratios_correctly(self):
        upcast_movies = self.test_movies \
            .withColumn('budget', col('budget').cast(DecimalType(15, 4))) \
            .withColumn('revenue', col('revenue').cast(DecimalType(15, 4)))

        actual_ratios = self.test_processor \
            ._calculate_revenue_budget_ratio(upcast_movies) \
            .select('revenue_budget_ratio', 'title') \
            .collect()

        expected_ratios = [
            Row(title='Executive Decision',
                revenue_budget_ratio=Decimal('2.22')),
            Row(title='Mission: Impossible II',
                revenue_budget_ratio=Decimal('4.37')),
            Row(title='Shalako', revenue_budget_ratio=Decimal('1.80')),
            Row(title='Anywhere But Here',
                revenue_budget_ratio=Decimal('0.81')),
            Row(title='The Strangers', revenue_budget_ratio=Decimal('9.15'))
        ]

        self.assertCountEqual(actual_ratios, expected_ratios)

    def test_picks_top_n_correctly(self):
        actual_movies = self.test_processor.top_n_movies(2).select(
            'title').collect()
        expected_movies = [
            Row(title='The Strangers'),
            Row(title='Mission: Impossible II')
        ]

        self.assertCountEqual(actual_movies, expected_movies)

    def test_returns_top_n_titles_correctly(self):
        actual_titles = self.test_processor.top_n_movie_titles(2)
        expected_titles = ['The Strangers', 'Mission: Impossible II']

        self.assertCountEqual(actual_titles, expected_titles)
