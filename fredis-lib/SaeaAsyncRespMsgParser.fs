
module SaeaAsyncRespMsgParser

open System
open FredisTypes
open FSharp.Control.AsyncSeqExtensions
open SocAsyncEventArgFuncs


// In pattern matching expressions, identifiers that begin with lowercase characters are always treated as 
// variables to be bound, rather than as literals, so you should generally use initial capitals when you define literals
// https://msdn.microsoft.com/en-us/library/dd233193.aspx

[<Literal>]
let SimpleStringL = 43  // +

[<Literal>]
let ErrorL = 45         // -

[<Literal>]
let IntegerL = 58       // :

[<Literal>]
let BulkStringL = 36    // $

[<Literal>]
let ArrayL = 42         // *

[<Literal>]
let CR = 13

[<Literal>]
let LF = 10


[<Literal>]
let CRb = 13uy





let private AsyncReadDelimitedResp (makeRESPMsg:Bytes -> Resp) (strm:IFredisStreamSource) : Async<Resp> = 
    async{
        let! bs = strm.AsyncReadUntilCRLF ()
        let resp = bs |> makeRESPMsg
        return resp
    }






// todo: currently AsyncReadInt64 will throw if reading invalid int64, consider returning an option
// todo: consider AsyncReadInt64 when profiling, would it be faster to perform arithmetic on bytes? if yes then use this as a reference impl
let private AsyncReadInt64 (strm:IFredisStreamSource) = async{
    let! bytes = strm.AsyncReadUntilCRLF ()
    let len = bytes.Length
    let mutable num = 0L
    let mutable ctr = 0

    if bytes.[0] = 45uy then //if the first byte is a '-' sign
        ctr <- ctr + 1
        while ctr < len do
            num <- num * 10L + (int64 bytes.[ctr]) - 48L
            ctr <- ctr + 1
        return num * -1L
    else
        while ctr < len do
            num <- num * 10L + (int64 bytes.[ctr]) - 48L
            ctr <- ctr + 1
        return num
}


// todo: currently AsyncReadInt32 will throw if reading invalid int64, consider returning an option
// todo: consider AsyncReadInt32 when profiling, would it be faster to perform arithmetic on bytes? if yes then use this as a reference impl
let private AsyncReadInt32 (strm:IFredisStreamSource) = async{
    let! ii64 = AsyncReadInt64 strm
    return int ii64
}



// copies from the stream directly into the array that will be used as a key or value
let private AsyncReadBulkString (strm:IFredisStreamSource) = 
    async{
        let! len = AsyncReadInt32 strm
        match len with
        | -1    ->  return Resp.BulkString BulkStrContents.Nil
        | len   ->  let! bytes =  strm.AsyncReadNBytes len
                    do!  strm.AsyncEatCRLF ()
                    return (bytes |> RespUtils.MakeBulkStr)
    }


let private AsyncReadRESPInteger strm = async{
    let! i64 = AsyncReadInt64 strm
    return (i64 |> Resp.Integer)
}



// LoadRESPMsgArray, LoadRESPMsgArray and LoadRESPMsgOuter are mutually recursive
// LoadRESPMsg is the parsing 'entry point' and is called after strm.AsyncReadByte fires (indicating there is a new RESP msg to parse)
let rec private LoadRESPMsgArray (strm:IFredisStreamSource) : Async<Resp> = 
    let asyncSeqResp =
        asyncSeq{
            let! numArrayElements = AsyncReadInt64 strm 
            for _ in 0L .. (numArrayElements - 1L) do
                let! msg = LoadRESPMsgInner strm
                yield msg
        } |> FSharp.Control.AsyncSeq.toArrayAsync

    async{
        let! msgs = asyncSeqResp
        return Resp.Array msgs
    }

and private LoadRESPMsgInner (strm:IFredisStreamSource) :Async<Resp> = async{
        let! bb = strm.AsyncReadByte ()
        let respTypeByte = bb |> int
        return! (LoadRESPMsg respTypeByte strm)
    }

and LoadRESPMsg (respType:int) (strm:IFredisStreamSource) : Async<Resp> =
    let ret = 
        match respType with
        | SimpleStringL -> AsyncReadDelimitedResp Resp.SimpleString strm
        | ErrorL        -> AsyncReadDelimitedResp Resp.Error strm
        | IntegerL      -> AsyncReadRESPInteger strm
        | BulkStringL   -> AsyncReadBulkString strm
        | ArrayL        -> LoadRESPMsgArray strm
        | _             -> failwith "invalid RESP" // need to escape from an arbitrary depth of recursion, hence throwing an exception
    ret



