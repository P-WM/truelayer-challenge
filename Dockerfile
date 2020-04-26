FROM godatadriven/pyspark

RUN apt-get update && apt-get install -y postgresql-client-11 wget

RUN mkdir -p /usr/local/app/datasets

RUN wget -nv https://www.dropbox.com/s/4gk0kijvkgo0s8g/enwiki.tar.gz \
    -O /usr/local/app/datasets/enwiki-latest-abstract.tar.gz
RUN tar -xzf /usr/local/app/datasets/enwiki-latest-abstract.tar.gz \
    -C /usr/local/app/datasets

RUN wget -nv https://www.dropbox.com/s/pv9etz9mgoge0v1/movies_metadata.csv \
    -O /usr/local/app/datasets/movies_metadata.csv

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . /usr/local/app

ENV PYTHONPATH /usr/local/app
