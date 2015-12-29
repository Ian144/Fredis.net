[<RequireQualifiedAccess>]
module AsyncRespMsgProcessor

open System.IO
open FredisTypes
open Utils

open FSharp.Control.AsyncSeq
open FSharp.Control.AsyncSeqExtensions



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






let rec AsyncReadUntilCRLF (ns:Stream) = async{
    let! bs = ns.AsyncRead(1) 
    match bs.[0] with
    | CRb   ->  let! lf = ns.AsyncRead(1)    // assuming the next char is LF, and eating it
                return []
    | hd    ->  let! tl = AsyncReadUntilCRLF ns      // TODO no TCO in AsyncReadUntilCRLF
                return (hd :: tl)
}



let AsyncReadDelimitedResp (makeRESPMsg:Bytes -> Resp) (strm:Stream) : Async<Resp> = async{
        let! bs = AsyncReadUntilCRLF strm
        let resp = bs |> Array.ofList |> makeRESPMsg
        return resp
    }




// an attempt at a functional int64 reader
let AsyncReadInt64 (strm:Stream) = async{
    let foldInt = (fun cur nxt -> cur * 10L + nxt)
    let asciiToDigit (asciiCode:byte) = (int64 asciiCode) - 48L
    let! bytes = AsyncReadUntilCRLF strm

    let ret = 
        if bytes.IsEmpty then
            0L
        else
            let sign, asciiCodes2 = 
                match bytes.Head with
                | 45uy  -> -1L, bytes.Tail
                | _     ->  1L, bytes
            let ii = asciiCodes2 |> List.map asciiToDigit |> List.fold foldInt 0L
            ii * sign
    return ret
}


// an attempt at a functional int64 reader
let AsyncReadInt32 (strm:Stream) = async{
    let foldInt = (fun cur nxt -> cur * 10 + nxt)
    let asciiToDigit (asciiCode:byte) = (int asciiCode) - 48
    let! bytes = AsyncReadUntilCRLF strm

    let ret = 
        if bytes.IsEmpty then
            0
        else
            let sign, asciiCodes2 = 
                match bytes.Head with
                | 45uy  -> -1, bytes.Tail
                | _     ->  1, bytes
            let ii = asciiCodes2 |> List.map asciiToDigit |> List.fold foldInt 0
            ii * sign
    return ret
}





// copies from the stream directly into the array that will be used as a key or value
let AsyncReadBulkString (rcvBufSz:int) (strm:Stream) = 

    let rec readNBytes (strm:Stream) (numBytesReadSoFar:int) (totalBytesToRead :int) (destArray:byte array) =
        async{
            let numBytesRemainingToRead = totalBytesToRead - numBytesReadSoFar 
            let numBytesToReadThisTime = if numBytesRemainingToRead > rcvBufSz then rcvBufSz else numBytesRemainingToRead
            let! numBytesRead = strm.AsyncRead (destArray, numBytesReadSoFar, numBytesToReadThisTime)
            let numBytesReadSoFar2 = numBytesReadSoFar + numBytesRead
            if numBytesReadSoFar2 > totalBytesToRead then 
                failwith "AsyncReadBulkString read more bytes than expected"
            if numBytesReadSoFar2 = totalBytesToRead then
                return ()
            else
                do! readNBytes strm numBytesReadSoFar2 totalBytesToRead destArray 
        }

    async{
        let! lenToRead = AsyncReadInt32 strm
        match lenToRead with
        | -1    ->  return Resp.BulkString BulkStrContents.Nil
        | len   ->  let bytes = Array.zeroCreate<byte> len
                    do! readNBytes strm  0 len bytes
                    let cr = strm.ReadByte()
                    let lf = strm.ReadByte()
                    return (bytes |> RespUtils.MakeBulkStr)
    }




let AsyncReadRESPInteger strm = async{
    let! i64 = AsyncReadInt64 strm
    return (i64 |> Resp.Integer)
}






// LoadRESPMsgArray, LoadRESPMsgArray and LoadRESPMsgOuter are mutually recursive
// LoadRESPMsg is the parsing 'entry point' and is called after ns.AsyncReadByte fires (indicating there is a new RESP msg to parse)
let rec LoadRESPMsgArray (rcvBuffSz:int) (ns:Stream) : Async<Resp> = 
    let asyncSeqResp =
        asyncSeq{
            let! numArrayElements = AsyncReadInt64 ns 
            for _ in 0L .. (numArrayElements - 1L) do
            let! msg = LoadRESPMsgInner rcvBuffSz ns
            yield msg
        }

    async{
        let! msgs = FSharp.Control.AsyncSeq.toArray asyncSeqResp
        return Resp.Array msgs
    }


and LoadRESPMsgInner (rcvBuffSz:int) (strm:Stream) :Async<Resp> = async{
        let! tmp = strm.AsyncRead(1)
        let respTypeByte = tmp.[0] |> int
        return! (LoadRESPMsg rcvBuffSz respTypeByte strm)
    }

and LoadRESPMsg (rcvBufSz:int) (respType:int) (strm:Stream) : Async<Resp> =
    match respType with
    | SimpleStringL -> AsyncReadDelimitedResp Resp.SimpleString strm
    | ErrorL        -> AsyncReadDelimitedResp Resp.Error strm
    | IntegerL      -> AsyncReadRESPInteger strm
    | BulkStringL   -> AsyncReadBulkString rcvBufSz strm
    | ArrayL        -> LoadRESPMsgArray rcvBufSz strm
    | _             -> failwith "invalid RESP" // need to escape from an arbitrary depth of recursion, hence throwing an exception
