
module AsyncRespStreamFuncs

open System
open System.IO
open FredisTypes

open SocAsyncEventArgFuncs


let EatCRLF (ns:Stream) = 
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore


let Eat1 (ns:Stream) = 
    ns.ReadByte() |> ignore



let Eat5NoAlloc (ns:Stream) = 
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore
    ns.ReadByte() |> ignore


// extension methods on Stream
type Stream with

    // read a single byte, return Option.None if client disconnected
    member this.AsyncReadByte buf = async{
        let tsk = this.ReadAsync(buf, 0, 1)
        let! numBytesRead = Async.AwaitTask tsk
        let ret = 
                match numBytesRead with
                | 0 -> None
                | _ -> Some buf.[0]
        return ret
        }



let private crlf        = "\r\n"B
let private simpStrType = "+"B
let private errStrType  = "-"B
let nilBulkStrBytes     = "$-1\r\n"B



let private AsyncSendBulkString (strm:IFredisStreamSink) (contents:BulkStrContents) =

    match contents with
    | BulkStrContents.Contents bs   ->  let prefix = (sprintf "$%d\r\n" bs.Length) |> Utils.StrToBytes
                                        async{
                                            do! strm.AsyncWriteBuf prefix
                                            do! strm.AsyncWriteBuf bs
                                            do! strm.AsyncWriteBuf crlf
                                        }
    | BulkStrContents.Nil         ->    async{ do! strm.AsyncWriteBuf nilBulkStrBytes }




//let private AsyncSendSimpleString2 (strm:IFredisStreamSink) (contents:byte array) =
//    async{
//        do! strm.AsyncWriteBuf simpStrType
//        do! strm.AsyncWriteBuf contents
//        do! strm.AsyncWriteBuf crlf
//    }

let private AsyncSendSimpleString (strm:IFredisStreamSink) (contents:byte array) =
    let len = 3 + contents.Length
    let arr = Array.zeroCreate<byte> len
    arr.[0] <- 43uy
    contents.CopyTo (arr,1)
    arr.[len-2] <- 13uy
    arr.[len-1] <- 10uy
    strm.AsyncWriteBuf arr



let AsyncSendError (strm:IFredisStreamSink) (contents:byte array) =
    async{
        do! strm.AsyncWriteBuf errStrType
        do! strm.AsyncWriteBuf contents
        do! strm.AsyncWriteBuf crlf 
    }


let private AsyncSendInteger (strm:IFredisStreamSink) (ii:int64) =
    let bs = sprintf ":%d\r\n" ii |> Utils.StrToBytes
    strm.AsyncWriteBuf bs
    

let rec AsyncSendResp (strm:IFredisStreamSink) (msg:Resp) =
    match msg with
    | Resp.Array arr            -> AsyncSendArray strm arr
    | Resp.BulkString contents  -> AsyncSendBulkString strm contents
    | Resp.SimpleString bs      -> AsyncSendSimpleString strm bs
    | Resp.Error err            -> AsyncSendError strm err
    | Resp.Integer ii           -> AsyncSendInteger strm ii

and private AsyncSendArray (strm:IFredisStreamSink) (arr:Resp []) =
    let lenBytes = sprintf "*%d\r\n" arr.Length |> Utils.StrToBytes
    let ctr = ref 0
    async{
        do! strm.AsyncWriteBuf( lenBytes)
        while !ctr < arr.Length do
            do! AsyncSendResp strm arr.[!ctr]
            ctr := !ctr + 1
    }
