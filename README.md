# TrueLayer challenge - TrueFilm analytics 🎬

## Using this project

### Prerequisites

In order to run this pipeline, you'll need the following:

- [docker](https://docs.docker.com/get-docker/)
- [docker-compose](https://docs.docker.com/compose/install/)

They both have easy installers and on most platforms the installation for the desktop client for `docker` includes `docker-compose`.

### Running the pipeline

TBC

## Choices

### Datasets

You will probably notice that the datasets for this pipeline come from different URLs to those specified. For the reasons below, I decided it was easier to download from versions I'd fetched and processed. Obviously in reality I would use some appropriate blob store (S3/GCS) rather than the super-professional, enterprise-grade, collaborative cloud document store Dropbox Personal (🤦‍♀️).

The movies metada from Kaggle was behind a registration wall (a prohibitively faffy step I wanted to avoid for the reviewer), so I made an easier public link for it.

The wikimedia dump was a large (~7.6GB extracted), XML file. This is pretty cumbersome for analytics so I took the fields we want (`title`, `abstract` and `url`) and dumped them to a columnar format. The process for downloading and transforming the wikimedia data is in the `staging` folder should you want to reproduce the process.
