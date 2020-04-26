from pyspark.sql import SparkSession
from os import path

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    datasets_path = f'{path.dirname(__file__)}/../datasets/'
    spark.read.parquet(f'{datasets_path}/enwiki').show()
    spark.read.csv(f'{datasets_path}/movies_metadata.csv',
                   header=True,
                   multiLine=True,
                   escape='"',
                   mode='FAILFAST').show()
