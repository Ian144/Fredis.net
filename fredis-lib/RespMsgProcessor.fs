[<RequireQualifiedAccess>]
module RespMsgProcessor

open System.IO
open RESPTypes
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


//#### this function is partial, so are all funcs which use it
let ReadUntilCRLF (ns:Stream) : int list = 
    let rec ReadInner (ns:Stream) bs : int list = 
        match ns.ReadByte() with
        | -1    ->  []                      // end of stream, #### reconsider if returning an empty list is correct
        | CR    ->  ns.ReadByte() |> ignore //#### assuming the next char is LF
                    bs
        | b     ->  b :: (ReadInner ns bs) 
    ReadInner ns []


// array length, string length etc 
//#### reconsider if it is correct behaviour to return zero when at end of the input stream
//#### this is a partial function, it must be able to fail, return an option?
let ReadInt32 (ns:Stream) = 
    let foldInt = (fun cur nxt -> cur * 10 + nxt)
    let AsciiDigitToDigit asciiCode = asciiCode - 48
    let raw = ReadUntilCRLF ns
    let digits = raw |> List.map AsciiDigitToDigit // convert ascii codes representing integer digits to the digits themselves
    List.fold foldInt 0 digits



let ReadLenPrefixedString (makeRESPMsg:Bytes -> RESPMsg) (ns:Stream) = 
    let length = ReadInt32 ns
    let bs = Array.zeroCreate<byte> length
    ns.Read (bs, 0, length) |> ignore //#### consider checking length read equals length expected
    EatCRLF ns
    makeRESPMsg bs


let ReadRESPInteger (ns:Stream) = 
    let ii = ReadInt32 ns  // will ParseLength read an int?
    RESPMsg.Integer ( System.Convert.ToInt64(ii) )


// LoadRESPMsgArray, LoadRESPMsgArray and LoadRESPMsgOuter are mutually recursive
// LoadRESPMsgOuter is the parsing 'entry point' and is called after ns.AsyncReadByte fires (indicating there is a new RESP msg to parse)
let rec LoadRESPMsgArray (ns:Stream) = 
    let numArrayElements = ReadInt32 ns
    let msgs = 
        [|  for _ in 0 .. (numArrayElements - 1) do
            yield (LoadRESPMsg ns) |] 
    RESPMsg.Array msgs

and LoadRESPMsg (ns:Stream) = 
    let respTypeByte = ns.ReadByte()
    LoadRESPMsgOuter respTypeByte ns

and LoadRESPMsgOuter (respTypeByte:int) (ns:Stream) = 
    match respTypeByte with
    | SimpleStringL -> ReadLenPrefixedString RESPMsg.SimpleString ns
    | ErrorL        -> ReadLenPrefixedString RESPMsg.Error ns
    | IntegerL      -> ReadRESPInteger ns
    | BulkStringL   -> ReadLenPrefixedString RESPMsg.BulkString ns
    | ArrayL        -> LoadRESPMsgArray ns
    | PingL         -> Eat5Bytes ns // redis-cli sends pings as PING\r\n - i.e. a raw string not RESP as i understand it
                       RESPMsg.BulkString pingBytes
    | _             -> failwith "invalid resp stream" //#### replace with an option or choice - how will this work with the mutual recursion??


// wraps LoadRESPMsgOuter so as to return a choice of 'processing complete' or failure (exception thrown) byte arrays
let LoadRESPMsgOuterChoice (respTypeByte:int) (ns:Stream) = 
        let funcx = FSharpx.Choice.protect (LoadRESPMsgOuter respTypeByte) 
        (funcx ns) |> FSharpx.Choice.mapSecond (fun exc -> StrToBytes exc.Message )
