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



//[<Property(Arbitrary = [|typeof<BufferSizes>|], Verbose=true)>]
[<Property(Arbitrary = [|typeof<BufferSizes>|])>]
let ``ReadBulkString output matches content`` (bufSize:int) (content:byte array) =
    //let bulkStrFormatBytes = content |> Utils.BytesToStr |> Utils.MakeRespBulkString |> Utils.StrToBytes
    let bulkStrFormatBytes = Utils.MakeBulkString content

    // drop the '$' that indicates that the contents of bulkStrFormatBytes is a BulkString
    let bulkStrFormatBytes2 = bulkStrFormatBytes |> Array.toList |> List.tail |> List.toArray

    use strm = new MemoryStream()
    strm.Write( bulkStrFormatBytes2, 0, bulkStrFormatBytes2.Length)
    let _ = strm.Seek(0L, System.IO.SeekOrigin.Begin)

    let _ = strm.Seek(0L, System.IO.SeekOrigin.Begin)

    let rm = RespMsgProcessor.ReadBulkString bufSize strm
    match rm with
    | SimpleString _ | Error _ | Integer _ | Array _ -> false
    | BulkString contentsOut   -> 
            let contentsInList = content |> List.ofArray
            let contentsOutList = contentsOut |> List.ofArray
            let ret = contentsInList = contentsOutList
            ret


//[<Property(Arbitrary = [|typeof<BufferSizes>|])>]
//let ``ReadBulkString always reads all of the streamed BulkString`` (bufSize:int) (content:byte array) =
////    let bulkStrFormatBytes = content |> Utils.BytesToStr |> Utils.MakeRespBulkString |> Utils.StrToBytes
////    use strm = new MemoryStream()
////    strm.Write( bulkStrFormatBytes, 0, bulkStrFormatBytes.Length)
////    let rm = RespMsgProcessor.ReadBulkString bufSize strm
////    match rm with
////    | SimpleString _ | Error _ | Integer _ | Array _ -> false
////    | BulkString contentsOut   -> 
////            let contentsInList = content |> List.ofArray
////            let contentsOutList = contentsOut |> List.ofArray
////            let ret = contentsInList = contentsOutList
////            ret
//    false



//let ``MakeBulkString byte manim equals MakeBulkStringOld string manip`` (content:byte array) =
//    let bs1 = Utils.MakeBulkString