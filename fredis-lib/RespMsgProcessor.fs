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




let ReadUntilCRLF (strm:Stream) : int list = 
    let rec ReadInner (ns:Stream) bs : int list = 
        match ns.ReadByte() with    // annoyingly ReadByte returns an int32
        | -1    ->  []              // end of stream, #### reconsider if returning an empty list is correct
        | CR    ->  Eat1 ns         //#### assuming the next char is LF
                    bs
        | b     ->  b :: (ReadInner ns bs) 
    ReadInner strm []



let ReadCRLFDelimitedStr (makeRESPMsg:Bytes -> Resp) (strm:Stream) : Resp = 
    let rec ReadInner (ns:Stream) bs : byte list = 
        match ns.ReadByte() with
        | -1    ->  []      // end of stream, #### reconsider if returning an empty list is correct
        | CR    ->  Eat1 ns //#### assuming the next char is LF
                    bs
        | ii    ->  let bb = System.Convert.ToByte ii
                    bb :: (ReadInner ns bs) 
    ReadInner strm [] |> Array.ofList |> makeRESPMsg



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


let ReadInt32 = ReadInt64 >> int32


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

    match lenToRead with
    | -1    ->  Resp.BulkString BulkStrContents.Nil
    | len   ->  let byteArr = Array.zeroCreate<byte> len
                do ReadInner strm  0 len byteArr
                EatCRLF strm
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
    | SimpleStringL -> ReadCRLFDelimitedStr Resp.SimpleString strm
    | ErrorL        -> ReadCRLFDelimitedStr Resp.Error strm
    | IntegerL      -> ReadRESPInteger strm
    | BulkStringL   -> ReadBulkString rcvBufSz strm
    | ArrayL        -> LoadRESPMsgArray rcvBufSz strm
    | _             -> failwith "invalid RESP" // need to escape from an arbitrary depth of recursion

