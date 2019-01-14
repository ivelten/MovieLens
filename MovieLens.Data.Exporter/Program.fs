open MovieLens.Data.Sql
open System

[<Literal>]
let private successCode = 0

[<Literal>]
let private unexpectedErrorCode = 1

[<Literal>]
let private invalidArgsErrorCode = 2

[<EntryPoint>]
let main args =
    match args with
    | [|connectionString|] ->
        printfn "Exporting data to Sql Server..."
        let exportTask = Export.toDatabase connectionString |> Async.StartAsTask
        try
            exportTask.GetAwaiter().GetResult()
            printfn "Data exported successfully to 'MovieLens' database."
            successCode
        with
        | :? AggregateException as ex ->
            ex.InnerExceptions |> Seq.iter (fun ex -> printfn "Unexpected error: %s" ex.Message)
            unexpectedErrorCode
        | ex ->
            printfn "Unexpected error: %s" ex.Message
            unexpectedErrorCode
    | _ -> 
        printfn "Invalid arguments."
        invalidArgsErrorCode
