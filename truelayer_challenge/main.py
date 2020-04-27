from pyspark.sql import SparkSession
from os import path
import logging

from truelayer_challenge.movie_processor import MovieProcessor
from truelayer_challenge.enwiki_matcher import EnwikiMatcher
from truelayer_challenge.models import create_all_tables, session, Movie

if __name__ == '__main__':
    logger = logging.getLogger('TrueFilm')
    logging.basicConfig()
    logger.setLevel(logging.INFO)

    logger.info("Creating DB tables")
    create_all_tables()

    logger.info("Beginning spark session")
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    datasets_path = f'{path.dirname(__file__)}/../datasets/'

    articles = spark.read.parquet(f'{datasets_path}/enwiki')
    movies = spark.read.csv(f'{datasets_path}/movies_metadata.csv',
                            header=True,
                            multiLine=True,
                            escape='"',
                            mode='FAILFAST')

    movie_processor = MovieProcessor(data=movies)
    enwiki_matcher = EnwikiMatcher(data=articles)

    top_1k_movies = movie_processor.top_n_movies(1000)
    matched = enwiki_matcher.join_with_movies(top_1k_movies)

    logger.info("Collecting results")
    movies_with_links = top_1k_movies.join(matched, 'movie_id',
                                           'left').collect()

    logger.info("Writing to DB")
    try:
        s = session()
        s.execute('TRUNCATE movies')
        for movie in movies_with_links:
            record = Movie(
                id=movie.movie_id,
                title=movie.title,
                budget=movie.budget,
                revenue=movie.revenue,
                ratio=movie.revenue_budget_ratio,
                production_companies=movie.production_companies,
                url=movie.url,
                abstract=movie.abstract,
            )
            s.add(record)
        s.commit()
    finally:
        s.close()
