﻿[<RequireQualifiedAccess>]
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
let PingL = 80          // P - redis-benchmark just sends PING\r\n, i.e. a raw string, not RESP as i understood it

[<Literal>]
let CR = 13

[<Literal>]
let LF = 10


let pingBytes   = Utils.StrToBytes "PING"



let ReadUntilCRLF (strm:Stream) : int list = 
    let rec ReadInner (ns:Stream) bs : int list = 
        match ns.ReadByte() with    // annoyingly ReadByte returns an int32
        | -1    ->  []       // end of stream, #### reconsider if returning an empty list is correct
        | CR    ->  Eat1 ns  //#### assuming the next char is LF
                    bs
        | b     ->  b :: (ReadInner ns bs) 
    ReadInner strm []



let ReadStringCRLF (makeRESPMsg:Bytes -> Resp) (strm:Stream) : Resp = 
    let rec ReadInner (ns:Stream) bs : byte list = 
        match ns.ReadByte() with
        | -1    ->  []      // end of stream, #### reconsider if returning an empty list is correct
        | CR    ->  Eat1 ns //#### assuming the next char is LF
                    bs
        | ii    ->  let bb = System.Convert.ToByte ii
                    bb :: (ReadInner ns bs) 
    ReadInner strm [] |> Array.ofList |> makeRESPMsg




//let ReadInt32 (strm:Stream) = 
//    let byteArray = ReadUntilCRLF strm |> List.map byte |> List.toArray
//    System.BitConverter.ToInt32 (byteArray, 0)
//
//let ReadInt64 (strm:Stream) = 
//    let byteArray = ReadUntilCRLF strm |> List.map byte |> List.toArray
//    System.BitConverter.ToInt64 (byteArray, 0)


// resp integers are streamed as delimited strings, not the byte array representing an int
let ReadInt32 (strm:Stream) = 
    let foldInt = (fun cur nxt -> cur * 10 + nxt)
    let AsciiDigitToDigit asciiCode = asciiCode - 48
    let asciiCodes = ReadUntilCRLF strm

    if asciiCodes.IsEmpty
    then    0
    else
            let sign, asciiCodes2 = 
                match asciiCodes.Head with
                | 45    -> -1, asciiCodes.Tail
                | _     ->  1, asciiCodes
            let ii1 = asciiCodes2 |> List.map AsciiDigitToDigit |> List.fold foldInt 0 
            ii1 * sign

let ReadInt64 (strm:Stream) = 
    let foldInt = (fun cur nxt -> cur * 10L + nxt)
    let AsciiDigitToDigit (asciiCode:int32) = (int64 asciiCode) - 48L
    let asciiCodes = ReadUntilCRLF strm

    if asciiCodes.IsEmpty
    then    0L
    else
            let sign, asciiCodes2 = 
                match asciiCodes.Head with
                | 45    -> -1L, asciiCodes.Tail
                | _     ->  1L, asciiCodes
            let ii1 = asciiCodes2 |> List.map AsciiDigitToDigit |> List.fold foldInt 0L
            ii1 * sign





// copies from the stream directly into the array that will be used as a key or value
let ReadBulkString (rcvBufSz:int) (strm:Stream) = 

    let rec ReadInner (strm:Stream) (numBytesReadSoFar:int) (totalBytesToRead:int) (byteArray:byte array) =
        let numBytesRemainingToRead = totalBytesToRead - numBytesReadSoFar 
        let numBytesToReadThisTime = if numBytesRemainingToRead > rcvBufSz then rcvBufSz else numBytesRemainingToRead
        let numBytesRead = strm.Read (byteArray, numBytesReadSoFar, numBytesToReadThisTime)
        let numBytesReadSoFar2 = numBytesReadSoFar + numBytesRead
        match numBytesReadSoFar2 with
        | num when num > totalBytesToRead ->    failwith "ReadBulkString read more bytes than expected"
        | num when num = totalBytesToRead ->    ()
        | _                               ->    ReadInner strm numBytesReadSoFar2 totalBytesToRead byteArray // ####TCO?

    let lenToRead = ReadInt32 strm
    let byteArr = Array.zeroCreate<byte> lenToRead
    do ReadInner strm  0 lenToRead byteArr
    EatCRLF strm
    Resp.BulkString byteArr



let ReadRESPInteger (ns:Stream) = 
    let ii = ReadInt64 ns  
    Resp.Integer ii



// LoadRESPMsgArray, LoadRESPMsgArray and LoadRESPMsgOuter are mutually recursive
// LoadRESPMsgOuter is the parsing 'entry point' and is called after ns.AsyncReadByte fires (indicating there is a new RESP msg to parse)
let rec LoadRESPMsgArray (rcvBuffSz:int) (ns:Stream) = 
    let numArrayElements = ReadInt64 ns 
    let msgs = 
        [|  for _ in 0L .. (numArrayElements - 1L) do
            yield (LoadRESPMsg rcvBuffSz ns) |] 
    Resp.Array msgs

and LoadRESPMsg (rcvBuffSz:int) (ns:Stream)  = 
    let respTypeByte = ns.ReadByte()
    LoadRESPMsgOuter rcvBuffSz respTypeByte ns 

and LoadRESPMsgOuter (rcvBufSz:int) (respTypeByte:int) (ns:Stream) = 
    match respTypeByte with
    | SimpleStringL -> ReadStringCRLF Resp.SimpleString ns
    | ErrorL        -> ReadStringCRLF Resp.Error ns
    | IntegerL      -> ReadRESPInteger ns
    | BulkStringL   -> ReadBulkString rcvBufSz ns
    | ArrayL        -> LoadRESPMsgArray rcvBufSz ns
    | PingL         -> Eat5Bytes ns // redis-cli sends pings as PING\r\n - i.e. a raw string not RESP (PING_INLINE is RESP)
                       Resp.BulkString pingBytes
    | _             -> failwith "invalid resp stream" 


// wraps LoadRESPMsgOuter so as to return a choice of 'processing complete' or failure (exception thrown) byte arrays
let LoadRESPMsgOuterChoice (rcvBuffSz:int) (respTypeByte:int) (ns:Stream)  = 
        let funcx = FSharpx.Choice.protect (LoadRESPMsgOuter rcvBuffSz respTypeByte) 
        (funcx ns) |> FSharpx.Choice.mapSecond (fun exc -> StrToBytes exc.Message )
