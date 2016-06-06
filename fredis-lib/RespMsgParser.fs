[<RequireQualifiedAccess>]
module RespMsgParser

open System.IO
open FredisTypes
//open Utils



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



// TODO reading a single byte at at time is probably inefficient, consider a more efficient version of this function
let rec ReadUntilCRLF (ns:Stream) : int list = 
    match ns.ReadByte() with    // annoyingly ReadByte returns an int32
    | -1    ->  failwith "ReadUntilCRLF EOS before CRLF"
    | CR    ->  RespStreamFuncs.Eat1 ns         // assuming the next char is LF, and eating it
                []
    | b     ->  b :: (ReadUntilCRLF ns ) 





let ReadDelimitedResp (makeRESPMsg:Bytes -> Resp) (strm:Stream) : Resp = 
    let bs = ReadUntilCRLF strm 
    bs |> List.map byte |> Array.ofList |> makeRESPMsg


// an imperative int64 reader, adapted from sider code
let inline ReadInt64 (strm:Stream) = 
    let mutable num = 0L
    let mutable b = strm.ReadByte()
    if b = 45 then // if first byte is a minus sign
        b <- strm.ReadByte()
        while b <> CR do
            num <- num * 10L + (int64 b) - 48L
            b <- strm.ReadByte()
        strm.ReadByte() |> ignore // throw away the CRLF
        num * -1L
    else
        while b <> CR do
            num <- num * 10L + (int64 b) - 48L
            b <- strm.ReadByte()
        strm.ReadByte() |> ignore // throw away the CRLF
        num



// an imperative int64 reader
// adapted from sider code
let inline ReadInt32 (strm:Stream) = 
    let mutable num = 0
    let mutable b = strm.ReadByte()
    if b = 45 then // if first byte is a '-' minus sign
        b <- strm.ReadByte()
        while b <> CR do
            num <- num * 10 + b - 48
            b <- strm.ReadByte()
        strm.ReadByte() |> ignore // throw away the CRLF
        num * -1
    else
        while b <> CR do
            num <- num * 10 + b - 48
            b <- strm.ReadByte()
        strm.ReadByte() |> ignore // throw away the CRLF
        num 


let ReadBulkString(rcvBufSz:int) (strm:Stream) = 

    let rec readInner (strm:Stream) (totalBytesToRead:int) (byteArray:byte array) =
        let mutable maxNumBytesToRead = if totalBytesToRead > rcvBufSz then rcvBufSz else totalBytesToRead
        let mutable totalSoFar = 0
        while totalSoFar < totalBytesToRead do
            let numBytesRead = strm.Read (byteArray, totalSoFar, maxNumBytesToRead)            
            totalSoFar <- totalSoFar + numBytesRead
            let numBytesRemaining = totalBytesToRead - totalSoFar
            maxNumBytesToRead <- if numBytesRemaining > rcvBufSz then rcvBufSz else numBytesRemaining
    
    let lenToRead = ReadInt32 strm

    match lenToRead with
    | -1    ->  Resp.BulkString BulkStrContents.Nil
    | len   ->  let byteArr = Array.zeroCreate<byte> len
                do readInner strm  len byteArr
                strm.ReadByte() |> ignore   // eat CR
                strm.ReadByte() |> ignore   // eat LF
                byteArr |> RespUtils.MakeBulkStr

let ReadRESPInteger = ReadInt64 >> Resp.Integer 
    



//let rec LoadRESPMsgArray (rcvBuffSz:int) (ns:Stream) = 
//    let numArrayElements = ReadInt64 ns 
//    let msgs = 
//        [|  for _ in 0L .. (numArrayElements - 1L) do
//            yield (LoadRESPMsgInner rcvBuffSz ns) |] 
//    Resp.Array msgs

let rec LoadRESPMsgArray (rcvBuffSz:int) (ns:Stream) = 
    let arrSz = ReadInt32 ns 
    let msgs = Array.zeroCreate<Resp> arrSz
    let maxIdx = arrSz - 1
    for idx = 0 to maxIdx do
        let resp = LoadRESPMsgInner rcvBuffSz ns
        msgs.[idx] <- resp
    Resp.Array msgs

and LoadRESPMsgInner (rcvBuffSz:int) (strm:Stream)  = 
    let respTypeByte = strm.ReadByte()
    LoadRESPMsg rcvBuffSz respTypeByte strm 

and LoadRESPMsg (rcvBufSz:int) (respType:int) (strm:Stream) = 
    match respType with
    | SimpleStringL ->  ReadDelimitedResp Resp.SimpleString strm
    | ErrorL        ->  ReadDelimitedResp Resp.Error strm
    | IntegerL      ->  ReadRESPInteger strm
    | BulkStringL   ->  ReadBulkString rcvBufSz strm
    | ArrayL        ->  LoadRESPMsgArray rcvBufSz strm
    | _             ->  let bs = Array.zeroCreate<byte> 16
                        strm.Read(bs, 0, 16) |> ignore
                        let str = bs |> Utils.BytesToStr  
                        let msg = sprintf "invalid RESP: %d - %s" respType str
                        failwith msg

