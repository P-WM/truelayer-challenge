from sys import argv
from pyspark.sql import SparkSession

if __name__ == "__main__":
    enwiki_path = argv[1]

    spark = SparkSession.builder.getOrCreate()

    enwiki_df = spark.read.format("xml").options(
        rowTag="doc").load(enwiki_path)

    enwiki_df.select("abstract", "title", "url").write.parquet("enwiki")
