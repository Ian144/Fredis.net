module RespStreamFuncs


open System.IO
open FredisTypes
open StructTuple


let EatCRLF (strm:Stream) = 
    strm.ReadByte() |> ignore
    strm.ReadByte() |> ignore


let Eat1 (strm:Stream) = 
    strm.ReadByte() |> ignore



let Eat5NoAlloc (strm:Stream) = 
    strm.ReadByte() |> ignore
    strm.ReadByte() |> ignore
    strm.ReadByte() |> ignore
    strm.ReadByte() |> ignore
    strm.ReadByte() |> ignore



  



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



let private AsyncSendBulkString (strm:Stream) (contents:BulkStrContents) =

    match contents with
    | BulkStrContents.Contents bs   ->  let prefix = (sprintf "$%d\r\n" bs.Length) |> Utils.StrToBytes
                                        async{
                                            do! strm.AsyncWrite prefix
                                            do! strm.AsyncWrite bs
                                            do! strm.AsyncWrite crlf
                                        }
    | BulkStrContents.Nil         ->    async{ do! strm.AsyncWrite nilBulkStrBytes }




let private AsyncSendSimpleString2 (strm:Stream) (contents:byte array) =
    async{
        do! strm.AsyncWrite simpStrType
        do! strm.AsyncWrite contents
        do! strm.AsyncWrite crlf
    }

let private AsyncSendSimpleString (strm:Stream) (contents:byte array) =
    let len = 3 + contents.Length
    let arr = Array.zeroCreate<byte> len
    arr.[0] <- 43uy
//    contents.CopyTo (arr,1)
    System.Buffer.BlockCopy(contents, 0, arr, 1, contents.Length )
    arr.[len-2] <- 13uy
    arr.[len-1] <- 10uy
    strm.AsyncWrite (arr, 0, len)



let AsyncSendError (strm:Stream) (contents:byte array) =
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
    | Resp.SimpleString bs      -> AsyncSendSimpleString strm bs
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
