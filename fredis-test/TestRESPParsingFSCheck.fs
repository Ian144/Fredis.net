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
        let! ii = Arb.Default.Int64().Generator
        return Resp.Integer ii
    }


let rec genRespArray = 
    gen{
        let! elements = Gen.arrayOfLength 4 genResp
        return Resp.Array elements
    }
and genResp = Gen.frequency [ 1, genRespSimpleString; 1, genRespError; 2, genRespInteger; 2, genRespBulkString; 1, genRespArray]


//ArbResp makes valid RESP only
type ArbResp = 
    static member Resp() = Arb.fromGen (genResp )

type BufferSizes = 
    static member Ints() =
        Arb.fromGen (Gen.choose(1, 8096*8096))





[<Property( Arbitrary=[|typeof<BufferSizes>; typeof<ArbResp>|] )>]
let ``Write-Read Resp stream roundtrip`` (bufSize:int) (respIn:Resp) =
    use strm = new MemoryStream()
    RespStreamFuncs.AsyncSendResp strm respIn |> Async.RunSynchronously
    strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
    let respTypeByte = strm.ReadByte() 
    let respOut = RespMsgProcessor.LoadRESPMsg bufSize respTypeByte strm
    let isEof = strm.Position = strm.Length
    respIn = respOut && isEof
    


[<Property>]
let ``ReadInt64 Write-Read roundtrip`` (ii:int64)  =
    use strm = new MemoryStream()
    let bytes = (sprintf "%d\r\n" ii) |> Utils.StrToBytes
    let _ = strm.Write (bytes, 0, bytes.Length)
    strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
    let iiOut = RespMsgProcessor.ReadInt64(strm)
    let isEof = strm.Position = strm.Length
    ii = iiOut && isEof

























