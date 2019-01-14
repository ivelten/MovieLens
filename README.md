# MovieLens data exporter

This is a tool generated in F#. It produces a Sql Server Database with imported data from [MovieLens Database](https://grouplens.org/datasets/movielens/), a sample large data set that can be used for tests and benchmarking.

## How to use this tool
1. Clone the repository, and build it.
2. Download the [full version of MovieLens sample database](http://files.grouplens.org/datasets/movielens/ml-latest.zip) into the [MovieLens.Data project folder](MovieLens.Data), and unzip it's files. It should create a sub folder named `ml-latest` inside, containing all .csv files of the original data set.
3. Install any version of Sql Server, and run `MovieLens.Data.Exporter`, passing a connection string to access it with privileges to create database and objects.
4. Wait until the application finishes. It should create a database named `MovieLens`, with all data ported to it - normalized and indexed.
5. Warning: The application will try to delete any previous database with the name `MovieLens`. If by any case you have a database with that name, don't run the application, or change the database name to not be changed by it.