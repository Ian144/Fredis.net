[<RequireQualifiedAccess>]
module RespMsgProcessor

open System.IO
open FredisTypes
open Utils



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



// #### reading a single byte at at time is probably inefficient, consider a more efficient version of this function
let rec ReadUntilCRLF (ns:Stream) : int list = 
    match ns.ReadByte() with    // annoyingly ReadByte returns an int32
    | -1    ->  failwith "ReadUntilCRLF EOS before CRLF"
    | CR    ->  RespStreamFuncs.Eat1 ns         // #### assuming the next char is LF, and eating it
                []
    | b     ->  b :: (ReadUntilCRLF ns ) 





let ReadDelimitedResp (makeRESPMsg:Bytes -> Resp) (strm:Stream) : Resp = 
    let bs = ReadUntilCRLF strm 
    bs |> List.map byte |> Array.ofList |> makeRESPMsg


// an attempt at a functional int64 reader
let ReadInt64F (strm:Stream) = 
    let foldInt = (fun cur nxt -> cur * 10L + nxt)
    let asciiDigitToDigit (asciiCode:int32) = (int64 asciiCode) - 48L
    let asciiCodes = ReadUntilCRLF strm

    if asciiCodes.IsEmpty then 0L
    else
        let sign, asciiCodes2 = 
            match asciiCodes.Head with
            | 45    -> -1L, asciiCodes.Tail
            | _     ->  1L, asciiCodes
        let ii1 = asciiCodes2 |> List.map asciiDigitToDigit |> List.fold foldInt 0L
        ii1 * sign


// an imperative int64 reader, adapted from sider code
let ReadInt64Imp (strm:Stream) = 
    let mutable num = 0L
    let mutable b = strm.ReadByte()

    if b = 45 then // if first byte is a '-' minus sign
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
let ReadInt32Imp (strm:Stream) = 
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

let ReadInt64 = ReadInt64Imp
let ReadInt32 = ReadInt32Imp

//let ReadInt64 = ReadInt64F
//let ReadInt32 = ReadInt64 >> int32


// copies from the stream directly into the array that will be used as a key or value
let ReadBulkString (rcvBufSz:int) (strm:Stream) = 

    let rec readInner (strm:Stream) (numBytesReadSoFar:int) (totalBytesToRead:int) (byteArray:byte array) =
        let numBytesRemainingToRead = totalBytesToRead - numBytesReadSoFar 
        let numBytesToReadThisTime = if numBytesRemainingToRead > rcvBufSz then rcvBufSz else numBytesRemainingToRead
        let numBytesRead = strm.Read (byteArray, numBytesReadSoFar, numBytesToReadThisTime)
        let numBytesReadSoFar2 = numBytesReadSoFar + numBytesRead
        match numBytesReadSoFar2 with
        | num when num > totalBytesToRead ->    failwith "ReadBulkString read more bytes than expected"
        | num when num = totalBytesToRead ->    ()
        | _                               ->    readInner strm numBytesReadSoFar2 totalBytesToRead byteArray // ####TCO?

    let lenToRead = ReadInt32 strm

    match lenToRead with
    | -1    ->  Resp.BulkString BulkStrContents.Nil
    | len   ->  let byteArr = Array.zeroCreate<byte> len
                do readInner strm  0 len byteArr
//                StreamFuncs.EatCRLF strm
                strm.ReadByte() |> ignore
                strm.ReadByte() |> ignore
                byteArr |> RespUtils.MakeBulkStr



let ReadRESPInteger = ReadInt64 >> Resp.Integer 
    



// LoadRESPMsgArray, LoadRESPMsgArray and LoadRESPMsgOuter are mutually recursive
// LoadRESPMsgOuter is the parsing 'entry point' and is called after ns.AsyncReadByte fires (indicating there is a new RESP msg to parse)
let rec LoadRESPMsgArray (rcvBuffSz:int) (ns:Stream) = 
    let numArrayElements = ReadInt64 ns 
    let msgs = 
        [|  for _ in 0L .. (numArrayElements - 1L) do
            yield (LoadRESPMsgInner rcvBuffSz ns) |] 
    Resp.Array msgs

and LoadRESPMsgInner (rcvBuffSz:int) (strm:Stream)  = 
    let respTypeByte = strm.ReadByte()
    LoadRESPMsg rcvBuffSz respTypeByte strm 

and LoadRESPMsg (rcvBufSz:int) (respType:int) (strm:Stream) = 
    match respType with
    | SimpleStringL -> ReadDelimitedResp Resp.SimpleString strm
    | ErrorL        -> ReadDelimitedResp Resp.Error strm
    | IntegerL      -> ReadRESPInteger strm
    | BulkStringL   -> ReadBulkString rcvBufSz strm
    | ArrayL        -> LoadRESPMsgArray rcvBufSz strm
    | _             -> failwith "invalid RESP" // need to escape from an arbitrary depth of recursion, hence throwing an exception

