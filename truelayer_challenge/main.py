from pyspark.sql import SparkSession
from os import path

from truelayer_challenge.movie_processor import MovieProcessor
from truelayer_challenge.enwiki_matcher import EnwikiMatcher

if __name__ == '__main__':
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

    movies_with_links = top_1k_movies.join(matched, 'movie_id', 'left')
    tops = movies_with_links.collect()

    print(tops)
