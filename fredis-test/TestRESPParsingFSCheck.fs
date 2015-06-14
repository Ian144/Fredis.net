module TestRESPParsingFSCheck



open System.IO
open Xunit
open FsCheck
open FsCheck.Xunit

open CmdCommon
open FredisTypes

[<Literal>]
let BulkStringL = 36    // $


[<Literal>]
let CR = 13

[<Literal>]
let LF = 10


type BufferSizes = 
    static member Ints() =
        Arb.fromGen (Gen.choose(1, 8096*8096))







[<Property(Arbitrary = [|typeof<BufferSizes>|])>]
let ``ReadBulkString from stream always consumes final CRLF`` (bufSize:int) (content:byte array) =
    use strm = new MemoryStream()
    do Utils.AsyncSendBulkString strm content |> Async.RunSynchronously
    strm.Seek(1L, System.IO.SeekOrigin.Begin) |> ignore
    let rm = RespMsgProcessor.ReadBulkString bufSize strm
    match rm with
    | SimpleString _ | Error _ | Integer _ | Array _ -> false
    | BulkString _  -> strm.Length = strm.Position // there should be nothing left to read 




[<Property(Arbitrary = [|typeof<BufferSizes>|])>]
let ``ReadBulkString from stream output matches content`` (bufSize:int) (content:byte array) =
    use strm = new MemoryStream()
    do Utils.AsyncSendBulkString strm content |> Async.RunSynchronously
    strm.Seek(1L, System.IO.SeekOrigin.Begin) |> ignore // seek to after the '$' RESP type character, as this will have been consumed before ReadBulkString is called
    let rm = RespMsgProcessor.ReadBulkString bufSize strm
    match rm with
    | SimpleString _ | Error _ | Integer _ | Array _ -> false
    | BulkString contentsOut   -> 
            let contentsInList = content |> List.ofArray    // convert to list as arrays do reference equality not value equality
            let contentsOutList = contentsOut |> List.ofArray
            contentsInList = contentsOutList
    

