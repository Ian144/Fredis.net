module RespStreamFuncs


open System.IO
open FredisTypes


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



  


let private crlf        = "\r\n"B
let private simpStrType = "+"B
let private errStrType  = "-"B
let nilBulkStrBytes     = "$-1\r\n"B



let private SendBulkString (strm:Stream) (contents:BulkStrContents) =
    match contents with
    | BulkStrContents.Contents bs   ->  let prefix = (sprintf "$%d\r\n" bs.Length) |> Utils.StrToBytes
                                        strm.Write (prefix, 0, prefix.Length)
                                        strm.Write (bs, 0, bs.Length)
                                        strm.Write (crlf, 0, 2)
    | BulkStrContents.Nil         ->    strm.Write (nilBulkStrBytes, 0, nilBulkStrBytes.Length)


let private SendSimpleString (strm:Stream) (contents:byte array) =
    strm.Write (simpStrType, 0, 1)
    strm.Write (contents, 0, contents.Length )
    strm.Write (crlf, 0, 2 )

//let private SendSimpleString (strm:Stream) (contents:byte array) =
//    let len = 3 + contents.Length
//    let arr = Array.zeroCreate<byte> len
//    arr.[0] <- 43uy
//    System.Buffer.BlockCopy(contents, 0, arr, 1, contents.Length )
//    arr.[len-2] <- 13uy
//    arr.[len-1] <- 10uy
//    strm.Write (arr, 0, len)


let SendError (strm:Stream) (contents:byte array) =
    strm.Write (errStrType, 0, 1)
    strm.Write (contents, 0, contents.Length )
    strm.Write (crlf, 0, 2 )


let private SendInteger (strm:Stream) (ii:int64) =
    let bs = sprintf ":%d\r\n" ii |> Utils.StrToBytes
    strm.Write (bs, 0, bs.Length)
    

let rec SendResp (strm:Stream) (msg:Resp) =
    match msg with
    | Resp.Array arr            -> SendArray strm arr
    | Resp.BulkString contents  -> SendBulkString strm contents
    | Resp.SimpleString bs      -> SendSimpleString strm bs
    | Resp.Error err            -> SendError strm err
    | Resp.Integer ii           -> SendInteger strm ii
and private SendArray (strm:Stream) (arr:Resp []) =
    let lenBytes = sprintf "*%d\r\n" arr.Length |> Utils.StrToBytes
    let ctr = ref 0
    strm.Write (lenBytes, 0, lenBytes.Length)
    while !ctr < arr.Length do
        SendResp strm arr.[!ctr]
        ctr := !ctr + 1
    