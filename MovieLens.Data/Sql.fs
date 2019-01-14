namespace MovieLens.Data.Sql

open MovieLens.Data
open System.Data.SqlClient

[<AutoOpen>]
module internal Helpers =
    let openConnection connectionString = async {
        let conn = new SqlConnection(connectionString)
        do! conn.OpenAsync() |> Async.AwaitTask
        return conn }

    let createCommand connection = new SqlCommand("", connection)

    let setParameters (parameters : seq<string * obj>) (command : SqlCommand) =
        command.Parameters.Clear()
        parameters |> Seq.iter (fun (name, value) -> command.Parameters.AddWithValue(name, value) |> ignore)
        command
    
    let executeNonQuery sql (command : SqlCommand) = async {
        command.CommandText <- sql
        do! command.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
        command.Parameters.Clear() }
    
    let executeScalar<'T> sql (command : SqlCommand) = async {
        command.CommandText <- sql
        let! res = command.ExecuteScalarAsync() |> Async.AwaitTask
        command.Parameters.Clear()
        return res :?> 'T }

module Export =
    let toDatabase connectionString = async {
        use! connection = openConnection connectionString
        use command = createCommand connection
        // Create database
        do! command |> executeNonQuery "USE master"
        let! databaseExists = command |> executeScalar<bool> "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'MovieLens') SELECT (CAST(0 AS BIT)); ELSE SELECT (CAST(1 AS BIT))"
        if databaseExists 
        then 
            do! command |> executeNonQuery "ALTER DATABASE MovieLens SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
            do! command |> executeNonQuery "DROP DATABASE MovieLens"
        do! command |> executeNonQuery "CREATE DATABASE MovieLens"
        do! command |> executeNonQuery "USE MovieLens"
        // Movies
        do! command |> executeNonQuery "CREATE TABLE Movies (MovieId INT NOT NULL PRIMARY KEY, Genres VARCHAR(255) NOT NULL, Title VARCHAR(255) NOT NULL)"
        let runSequentially =
            Seq.reduce (fun fst snd -> async { 
                    do! fst
                    return! snd })
        let! movies = Movies.AsyncGetSample()
        do!
            movies.Rows
            |> Seq.map (fun movie -> async {
                do!
                    command
                    |> setParameters ["@MovieId", upcast movie.MovieId; "@Genres", upcast movie.Genres; "@Title", upcast movie.Title]
                    |> executeNonQuery "INSERT INTO Movies (MovieId, Genres, Title) VALUES (@MovieId, @Genres, @Title)" })
            |> runSequentially
        // Ratings
        do! command |> executeNonQuery "CREATE TABLE Ratings (
	        MovieId INT NOT NULL FOREIGN KEY REFERENCES Movies(MovieId),
	        UserId INT NOT NULL,
	        Rating DECIMAL NOT NULL,
	        [Timestamp] INT NOT NULL,
	        PRIMARY KEY (MovieId, UserId))"
        let! ratings = Ratings.AsyncGetSample()
        do!
            ratings.Rows
            |> Seq.map (fun rating -> async {
                do!
                    command
                    |> setParameters ["@MovieId", upcast rating.MovieId; "@UserId", upcast rating.UserId; "@Rating", upcast rating.Rating; "@Timestamp", upcast rating.Timestamp]
                    |> executeNonQuery "INSERT INTO Ratings (MovieId, UserId, Rating, [Timestamp]) VALUES (@MovieId, @UserId, @Rating, @Timestamp)" })
            |> runSequentially
        // Tags
        do! command |> executeNonQuery "CREATE TABLE Tags (
	        MovieId INT NOT NULL FOREIGN KEY REFERENCES Movies(MovieId),
	        UserId INT NOT NULL,
	        Tag VARCHAR(255) NOT NULL,
	        [Timestamp] INT NOT NULL,
	        PRIMARY KEY (MovieId, UserId))"
        let! tags = Tags.AsyncGetSample()
        do!
            tags.Rows
            |> Seq.map (fun tag -> async {
            do!
                command
                |> setParameters ["@MovieId", upcast tag.MovieId; "@UserId", upcast tag.UserId; "@Tag", upcast tag.Tag; "@Timestamp", upcast tag.Timestamp]
                |> executeNonQuery "INSERT INTO Tags (MovieId, UserId, Tag, [Timestamp]) VALUES (@MovieId, @UserId, @Tag, @Timestamp)" })
            |> runSequentially
        // Links
        do! command |> executeNonQuery "CREATE TABLE Links (
	        MovieId INT NOT NULL FOREIGN KEY REFERENCES Movies(MovieId),
	        ImdbId INT NOT NULL,
	        TmdbId INT NOT NULL,
	        PRIMARY KEY (MovieId),
	        UNIQUE (ImdbId),
	        UNIQUE (TmdbId))"
        let! links = Links.AsyncGetSample()
        do!
            links.Rows
            |> Seq.map (fun link -> async {
                do!
                    command
                    |> setParameters ["@MovieId", upcast link.MovieId; "@ImdbId", upcast link.ImdbId; "@TmdbId", upcast link.TmdbId]
                    |> executeNonQuery "INSERT INTO Links (MovieId, ImdbId, TmdbId) VALUES (@MovieId, @ImdbId, @TmdbId)" })
            |> runSequentially
        // GenomeTags
        do! command |> executeNonQuery "CREATE TABLE GenomeTags (TagId INT NOT NULL PRIMARY KEY, Tag VARCHAR(255) NOT NULL)"
        let! genomeTags = GenomeTags.AsyncGetSample()
        do!
            genomeTags.Rows
            |> Seq.map (fun genomeTag -> async {
                do!
                    command
                    |> setParameters ["@TagId", upcast genomeTag.TagId; "@Tag", upcast genomeTag.Tag]
                    |> executeNonQuery "INSERT INTO GenomeTags (TagId, Tag) VALUES (@TagId, @Tag)" })
            |> runSequentially
        // GenomeScores
        do! command |> executeNonQuery "CREATE TABLE GenomeScores (
	        MovieId INT NOT NULL FOREIGN KEY REFERENCES Movies(MovieId),
	        TagId INT NOT NULL FOREIGN KEY REFERENCES GenomeTags(TagId),
	        Relevance DECIMAL NOT NULL
	        PRIMARY KEY (MovieId, TagId))"
        let! genomeScores = GenomeScores.AsyncGetSample()
        do!
            genomeScores.Rows
            |> Seq.map (fun genomeScore -> async {
                do!
                    command
                    |> setParameters ["@MovieId", upcast genomeScore.MovieId; "@TagId", upcast genomeScore.TagId; "@Relevance", upcast genomeScore.Relevance]
                    |> executeNonQuery "INSERT INTO GenomeScores(MovieId, TagId, Relevance) VALUES (@MovieId, @TagId, @Relevance)" })
            |> runSequentially }

