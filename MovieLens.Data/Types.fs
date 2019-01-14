namespace MovieLens.Data

open FSharp.Data

type GenomeScores = CsvProvider<"ml-latest/genome-scores.csv">
type GenomeTags = CsvProvider<"ml-latest/genome-tags.csv">
type Links = CsvProvider<"ml-latest/links.csv">
type Movies = CsvProvider<"ml-latest/movies.csv">
type Ratings = CsvProvider<"ml-latest/ratings.csv">
type Tags = CsvProvider<"ml-latest/tags.csv">