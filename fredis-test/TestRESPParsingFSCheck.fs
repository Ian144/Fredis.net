module TestRESPParsingFSCheck



open System.IO
open Xunit
open FsCheck
open FsCheck.Xunit

open Utils
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

//type RespArb = 
//    static member Resp.SimpleString() =
//        let bytes = (Gen.choose)
//        Arb.fromGen (Gen.choose(1, 8096*8096))




//let genAlphaByte () = Gen.choose(65,122) |> Gen.map byte
//let genAlphaByteArray () = Gen.arrayOf (genAlphaByte ())

// 65, and 122 are the lowest and highest alpha characters
// genAlphaByteArray is used to create the contents of Resp SimpleString's and Errors, which cannot contain CRLF
let genAlphaByte = Gen.choose(65,122) |> Gen.map byte 
let genAlphaByteArray = Gen.arrayOf genAlphaByte 


let genRespBulkString = 
    gen{
        let! bytes = genAlphaByteArray    
        return Resp.BulkString bytes
    }


let genRespSimpleString = 
    gen{
        let! bytes = genAlphaByteArray    
        return Resp.SimpleString bytes
    }

let genRespError = 
    gen{
        let! bytes = genAlphaByteArray    
        return Resp.Error bytes
    }


let genRespInteger = 
    gen{
//        let! yy = Arb.Default.DontSizeInt64().Generator
//        let xx = yy.unwrap
        let! ii = Arb.Default.Int64().Generator
        return Resp.Integer ii
    }


let rec genRespArray = 
    gen{
        let! elements = Gen.arrayOfLength 4 genResp
        return Resp.Array elements
    }
and genResp = Gen.oneof [genRespSimpleString; genRespError; genRespInteger; genRespBulkString; genRespArray]

type ArbResp = 
    static member Resp() = Arb.fromGen (genResp )




// delimited strings cannot contain CR or LF
//  create an Arbitrary that does not add them
//  filter them out   

// add buf size as a parameter one the rest work

//[<Property(Verbose=true)>]
//[<Property( Arbitrary=[|typeof<ArbResp>|], Verbose = true, MaxTest= 999 )>]
[<Property( Arbitrary=[|typeof<BufferSizes>; typeof<ArbResp>|], MaxTest= 999 )>]
let ``Write-Read Resp stream roundtrip`` (bufSize:int) (respIn:Resp) =
    try
        use strm = new MemoryStream()
        Utils.AsyncSendResp strm respIn |> Async.RunSynchronously
        strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore

        let respTypeByte = strm.ReadByte() 
        let respOut = RespMsgProcessor.LoadRESPMsgOuter bufSize respTypeByte strm
        if respIn = respOut then
            true
        else
            false
    with
    | ex    ->  printfn "%s" ex.Message
                false






[<Property(Arbitrary = [|typeof<BufferSizes>|])>]
let ``ReadBulkString from stream output matches content`` (bufSize:int) (content:byte array) =
    use strm = new MemoryStream()
    do Utils.AsyncSendResp strm (Resp.BulkString content) |> Async.RunSynchronously
    strm.Seek(1L, System.IO.SeekOrigin.Begin) |> ignore // seek to after the '$' RESP type character, as this will have been consumed before ReadBulkString is called
    let rm = RespMsgProcessor.ReadBulkString bufSize strm
    match rm with
    | SimpleString _ | Error _ | Integer _ | Array _ -> false
    | BulkString contentsOut   -> 
            let contentsInList = content |> List.ofArray    // convert to list as arrays do reference equality not value equality
            let contentsOutList = contentsOut |> List.ofArray
            contentsInList = contentsOutList


[<Property(Arbitrary = [|typeof<BufferSizes>|])>]
let ``ReadBulkString from stream always consumes final CRLF`` (bufSize:int) (content:byte array) =
    use strm = new MemoryStream()
    let bulkStr = Resp.BulkString content
    do Utils.AsyncSendResp strm bulkStr |> Async.RunSynchronously
    strm.Seek(1L, System.IO.SeekOrigin.Begin) |> ignore
    let rm = RespMsgProcessor.ReadBulkString bufSize strm
    match rm with
    | SimpleString _ | Error _ | Integer _ | Array _ -> false
    | BulkString _  -> strm.Length = strm.Position // there should be nothing left to read 

