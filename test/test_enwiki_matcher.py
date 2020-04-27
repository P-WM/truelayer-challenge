from unittest import TestCase
from pyspark.sql import SparkSession, Row

from truelayer_challenge.enwiki_matcher import EnwikiMatcher

from test.fixtures import articles, processed_movies


class TestEnwikiMatcher(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName(
            "TestEnwikiMatcher").getOrCreate()

        cls.test_articles = cls.spark.createDataFrame(articles)

    def setUp(self):
        self.test_matcher = EnwikiMatcher(data=self.test_articles)

    def test_matches_film_pages_preferentially(self):
        movies_df = self.spark.createDataFrame(processed_movies)
        actual_articles = self.test_matcher.join_with_movies(movies_df).select(
            'movie_id', 'url').collect()

        expected_articles = [
            Row(movie_id='FFFFFFFFBF6AD93F',
                url='https://en.wikipedia.org/wiki/Executive_Decision'),
            Row(movie_id='752E4DDB',
                url=
                'https://en.wikipedia.org/wiki/Anywhere_but_Here_(1999 film)'),
            Row(movie_id='FFFFFFFFB28C5FBD',
                url='https://en.wikipedia.org/wiki/Shalako_(film)'),
            Row(movie_id='46A353C9',
                url='https://en.wikipedia.org/wiki/The_Strangers_(2008_film)')
        ]

        self.assertCountEqual(actual_articles, expected_articles)

    def test_ignores_case(self):
        pass

    def test_tidies_names(self):
        pass
