# TrueLayer challenge - TrueFilm™️ analytics 🎬

## Using this project

### Prerequisites

In order to run this pipeline, you'll need the following:

- [docker](https://docs.docker.com/get-docker/)
- [docker-compose](https://docs.docker.com/compose/install/)

They both have easy installers and on most platforms the installation for the desktop client for `docker` includes `docker-compose`.

### Running the pipeline

With the above installed, you can start the TrueFilm™️ pipeline by running the following command in the root directory of the project:

```bash
docker-compose up --build
```

I'm afraid it will be slow to build the first time, as it needs to download ~550MB worth of data but should be fine after that.

### Viewing results

`adminer` is running which will let you see the results.

You can access it from your browser at `localhost:8080`. You will need the following to login:

- System: `PostgreSQL`
- Server: `postgres`
- Username: `AzureDiamond`
- Password: [hunter2](https://knowyourmeme.com/memes/hunter2)
- Databse: `true_film`

## Choices

### Datasets

You will probably notice that the datasets for this pipeline come from different URLs to those specified. For the reasons below, I decided it was easier to download from versions I'd fetched and processed. Obviously in reality I would use some appropriate blob store (S3/GCS) rather than the super-professional, enterprise-grade, collaborative cloud document store Dropbox Personal (🤦‍♀️).

The movies metada from Kaggle was behind a registration wall (a prohibitively faffy step I wanted to avoid for the reviewer), so I made an easier public link for it.

The wikimedia dump was a large (~7.6GB extracted), XML file. This is pretty cumbersome for analytics so I took the fields we want (`title`, `abstract` and `url`) and wrote them out to a columnar format. The process for downloading and transforming the wikimedia data is in the `staging` folder should you want to reproduce the process.
