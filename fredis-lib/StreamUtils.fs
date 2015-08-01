
module StreamFuncs

open System
open System.IO
open FredisTypes







let EatCRLF (ns:Stream) = 
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore


let Eat1 (ns:Stream) = 
    ns.ReadByte() |> ignore



let Eat5NoArray (ns:Stream) = 
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore

let Eat13NoArray (ns:Stream) = 
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore



// extension methods on NetworkStream
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

    member ns.AsyncReadByte3 bytes = async{
        let tsk = ns.ReadAsync(bytes, 0, 1)
        let! numBytesRead = Async.AwaitTask tsk
        let ret = 
                match numBytesRead with
                | 0 -> None
                | _ -> Some bytes.[0]
        return ret
        }








let private crlf = [|13uy; 10uy |]
let private simpStrType = "+" |> Utils.StrToBytes
let private errStrType = "-"  |> Utils.StrToBytes


let nilBulkStrBytes = "$-1\r\n" |> Utils.StrToBytes



let private AsyncSendBulkString (strm:Stream) (contents:BulkStrContents) =

    match contents with
    | BulkStrContents.Contents bs   ->  let bulkStrPrefixAndLength = (sprintf "$%d\r\n" bs.Length) |> Utils.StrToBytes
                                        async{
                                            do! strm.AsyncWrite bulkStrPrefixAndLength
                                            do! strm.AsyncWrite bs
                                            do! strm.AsyncWrite crlf
                                        }
    | BulkStrContents.Nil         ->    async{ do! strm.AsyncWrite nilBulkStrBytes }


let pongBytes  = Utils.StrToBytes "+PONG\r\n"

let private AsyncSendSimpleString (strm:Stream) (contents:byte array) =
//    strm.AsyncWrite pongBytes // 
    async{
        do! strm.AsyncWrite simpStrType
        do! strm.AsyncWrite contents
        do! strm.AsyncWrite crlf
    }

let private AsyncSendSimpleString2 (destStrm:Stream) (contents:byte array) =
    let len = 3 + contents.Length
    let arr = Array.zeroCreate<byte> len
    arr.[0] <- 43uy
    contents.CopyTo (arr,1)
    arr.[len-2] <- 13uy
    arr.[len-1] <- 10uy
    destStrm.AsyncWrite (arr, 0, len)




let private AsyncSendError (strm:Stream) (contents:byte array) =
    async{
        do! strm.AsyncWrite errStrType
        do! strm.AsyncWrite contents
        do! strm.AsyncWrite crlf
    }


let private AsyncSendInteger (strm:Stream) (ii:int64) =
    let bs = sprintf ":%d\r\n" ii |> Utils.StrToBytes
    strm.AsyncWrite bs


let rec AsyncSendResp (strm:Stream) (msg:Resp) = 
    match msg with
    | Resp.Array arr            -> AsyncSendArray strm arr
    | Resp.BulkString contents  -> AsyncSendBulkString strm contents
    | Resp.SimpleString bs      -> AsyncSendSimpleString2 strm bs
    | Resp.Error err            -> AsyncSendError strm err
    | Resp.Integer ii           -> AsyncSendInteger strm ii

and private AsyncSendArray (strm:Stream) (arr:Resp []) =
    let lenBytes = sprintf "*%d\r\n" arr.Length |> Utils.StrToBytes  
    let ctr = ref 0
    async{
        do! strm.AsyncWrite( lenBytes)
        while !ctr < arr.Length do
            do! AsyncSendResp strm arr.[!ctr]
            ctr := !ctr + 1
    }
