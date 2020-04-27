from pyspark.sql import DataFrame

from pyspark.sql.functions import regexp_extract, concat


class EnwikiMatcher:
    def __init__(self, *, data: DataFrame):
        self.raw_articles = data
        self.processed_articles = None

    @staticmethod
    def _clean_articles(articles: DataFrame) -> DataFrame:
        return articles.withColumn(
            regexp_extract(articles.title, r"^Wikipedia: (.+)", 1))

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
        such as release date, country, language etc.

        It would also be sensible to preprocess the articles, pick out those on films
        and surface useful metadata for matching.
        """
        # First pick all articles explicitly about films
        explicitly_film_articles = self.all_articles \
            .join(movies, self.all_articles.title == concat(movies.title, ' (film)'))

        # Add all articles with the same name as movies (but not in the above)
        same_title_articles = self.all_articles \
            .join(movies, 'title') \
            .join(explicitly_film_articles, 'movie_id', 'left_anti')

        return movies \
            .join(same_title_articles, 'movie_id', 'left')
