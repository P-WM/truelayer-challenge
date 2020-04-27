from pyspark.sql import DataFrame

from pyspark.sql.functions import regexp_extract, col, concat, lit, lower


class EnwikiMatcher:
    def __init__(self, *, data: DataFrame):
        self.raw_articles = data
        self.processed_articles = None

    @staticmethod
    def _clean_articles(articles: DataFrame) -> DataFrame:
        return articles \
            .withColumn('wiki_title', regexp_extract(articles.title, r"^Wikipedia: (.+)", 1)) \
            .drop('title')

    @property
    def all_articles(self):
        if self.processed_articles:
            return self.processed_articles

        processed_articles = self.raw_articles

        process_steps = [self._clean_articles]

        for step in process_steps:
            processed_articles = step(processed_articles)

        self.processed_articles = processed_articles

        return processed_articles

    def join_with_movies(self, movies: DataFrame) -> DataFrame:
        """
        There's very rudimentary logic here which simply prefers WikiPedia articles with
        the title: "<movie_title> (film)"

        Clearly, a much better rule set could be used here that used other information
        such as country, language etc.

        It would also be sensible to preprocess the articles, pick out those on films
        and surface useful metadata for matching. We could then give some useful hints to speed things up
        by partitioning by the info we wanted.
        """
        movies = movies.select('title', 'year', 'movie_id')

        # First try articles about a film in that year
        correct_year_film_articles = self.all_articles \
            .join(movies, lower(self.all_articles.wiki_title) == lower(concat(movies.title, lit(' ('), movies.year, lit(' film)')))) \
            .select('movie_id', 'abstract', 'url', 'wiki_title', 'title', 'year')

        # Next add all articles explicitly about films
        explicitly_film_articles = self.all_articles \
            .join(movies, lower(self.all_articles.wiki_title) == lower(concat(movies.title, lit(' (film)')))) \
            .join(correct_year_film_articles, 'movie_id', 'left_anti') \
            .union(correct_year_film_articles)

        # Finally add all articles with the same name as movies (but not in the above)
        same_title_articles = self.all_articles \
            .join(movies, lower(self.all_articles.wiki_title) == lower(movies.title)) \
            .join(explicitly_film_articles, 'movie_id', 'left_anti') \
            .union(explicitly_film_articles)

        all_film_articles = same_title_articles.select('movie_id', 'url',
                                                       'abstract')

        return all_film_articles
