﻿module Utils

//#### use less generic name than Utils and/or separate into multiple files

open System
open System.IO

open FredisTypes



let BytesToStr bs = System.Text.Encoding.UTF8.GetString(bs)
let StrToBytes (str:string) = System.Text.Encoding.UTF8.GetBytes(str)   
let BytesToInt64 bs = System.BitConverter.ToInt64(bs, 0)
let BytesToKey = BytesToStr >> Key





let EatCRLF (ns:Stream) = 
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore


let Eat1 (ns:Stream) = 
    ns.ReadByte() |> ignore


let Eat3 (ns:Stream) = 
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore


let EatNBytes (len) (ns:Stream) = 
    let bs = Array.zeroCreate<byte> len
    ns.Read(bs, 0, len) |> ignore
    ()


let Eat5Bytes ns = EatNBytes 5 ns


// async extension methods on NetworkStream
type Net.Sockets.NetworkStream with

    // read a single byte, throw if client disconnected
    member ns.AsyncReadByte () = async{
        let! bs = ns.AsyncRead (1)
        return bs.[0]
        }

    // read a single byte,  returns 'None' when there is nothing to read - probably due to client shutting down without closing the socket
    member ns.AsyncReadByte2 () = async{
        let bytes = Array.create 1 0uy
        let tsk = ns.ReadAsync(bytes, 0, 1)
        let! numBytesRead = Async.AwaitTask tsk
        let ret = 
                match numBytesRead with
                | 0 -> None
                | _ -> Some bytes.[0]
        return ret
        }




let AsyncSendBulkString (strm:Stream) (contents:byte array) =
    let bulkStrPrefixAndLength = (sprintf "$%d\r\n" contents.Length) |> StrToBytes
    let crlf = [|13uy; 10uy |]
    async{
        do! strm.AsyncWrite( bulkStrPrefixAndLength ) 
        do! strm.AsyncWrite( contents )
        do! strm.AsyncWrite( crlf )
    }




let MakeArraySingleRespBulkString (ss:string) = sprintf "*1\r\n$%d\r\n%s\r\n" ss.Length ss


//let MakeRespIntegerArr (ii:int64) = 
//    let ss = sprintf "*1\r\n:%d\r\n" ii
//    StrToBytes ss


let SetBit (bs:byte []) (index:int) (value:bool) =
    let byteIndex   = index / 8
    let bitIndex    = index % 8
    let mask = 1uy <<< bitIndex
    match value with
    | true  ->  bs.[byteIndex] <- bs.[byteIndex] ||| mask
                ()
    | false ->  bs.[byteIndex] <- bs.[byteIndex] &&& (~~~mask)
                ()


let GetBit (bs:byte []) (index:int) : bool =
    let byteIndex   = index / 8
    let bitIndex    = index % 8
    let mask = 1uy <<< bitIndex
    (bs.[byteIndex] &&& mask) <> 0uy



let OptionToChoice (optFunc:'a -> 'b option) (xx:'a) choice2Of2Val  = 
    match optFunc xx with
    | Some yy   -> Choice1Of2 yy
    | None      -> Choice2Of2 choice2Of2Val
                    
                    
let ChoiceParseInt failureMsg str :Choice<int,byte[]> = OptionToChoice FSharpx.FSharpOption.ParseInt str failureMsg


let ChoiceParseBoolFromInt (errorMsg:byte[]) (ii:int) = 
    match ii with
    | 1 -> Choice1Of2 true
    | 0 -> Choice1Of2 false
    | _ -> Choice2Of2 errorMsg


let ChoiceParseBool (errorMsg) (ss) = 
    match ss with
    | "1"   -> Choice1Of2 true
    | "0"   -> Choice1Of2 false
    | _     -> Choice2Of2 errorMsg


