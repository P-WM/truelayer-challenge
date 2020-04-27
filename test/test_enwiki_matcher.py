from unittest import TestCase
from pyspark.sql.types import


class TestEnwikiMatcher(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName(
            "TestEnwikiMatcher").getOrCreate()

    def test_matches_film_pages_preferentially(self):
        pass

    def test_does_not_drop_movies_with_no_article(self):
        pass

    def test_tidies_names(self):
        pass


