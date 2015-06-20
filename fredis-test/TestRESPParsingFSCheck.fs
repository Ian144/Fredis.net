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



let genPopulatedRespBulkString = 
    gen{
        let! bytes = genAlphaByteArray    
        return RespUtils.MakeBulkStr bytes
    }

let genNilRespBulkStr = gen{ return Resp.BulkString BulkStrContents.Nil }



let genRespBulkString = Gen.frequency [10, genPopulatedRespBulkString; 
                                       1,  genNilRespBulkStr]


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
and genResp = Gen.frequency [ 1, genRespSimpleString; 1, genRespError; 2, genRespInteger; 2, genRespBulkString; 2, genRespArray]

type ArbResp = 
    static member Resp() = Arb.fromGen (genResp )




//ArbResp makes valid RESP only

[<Property( Arbitrary=[|typeof<BufferSizes>; typeof<ArbResp>|] )>]
//[<Property( Arbitrary=[|typeof<BufferSizes>; typeof<ArbResp>|], MaxTest = 999 )>]
let ``Write-Read Resp stream roundtrip`` (bufSize:int) (respIn:Resp) =
    use strm = new MemoryStream()
    Utils.AsyncSendResp strm respIn |> Async.RunSynchronously
    strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
    let respTypeByte = strm.ReadByte() 
    let respOut = RespMsgProcessor.LoadRESPMsgOuter bufSize respTypeByte strm
    let isEof = strm.Position = strm.Length
    respIn = respOut && isEof   
    


