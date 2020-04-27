from pyspark.sql import SparkSession
from os import path

from truelayer_challenge.movie_processor import MovieProcessor

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    datasets_path = f'{path.dirname(__file__)}/../datasets/'

    articles = spark.read.parquet(f'{datasets_path}/enwiki').show()
    movies = spark.read.csv(f'{datasets_path}/movies_metadata.csv',
                            header=True,
                            multiLine=True,
                            escape='"',
                            mode='FAILFAST')
