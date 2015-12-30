[<RequireQualifiedAccess>]
module RespUtils


open FredisTypes




let RespStrLen (str:string) = Resp.Integer (int64 str.Length)

let MakeBulkStr = BulkStrContents.Contents >> Resp.BulkString

let emptyBulkStr = Resp.BulkString (BulkStrContents.Contents [||]) // i.e. an empty byte array
let nilBulkStr = Resp.BulkString BulkStrContents.Nil

let errorBytes  = "-Error\r\n"B

let okSimpleStr     = "OK"B     |> Resp.SimpleString
let pingSimpleStr   = "PING"B   |> Resp.SimpleString
let pongSimpleStr   = "PONG"B   |> Resp.SimpleString




// TODO remove the need for partial functions
let PartialGetMsgPayload respMsg = 
    match respMsg with
    | Resp.Array _              ->  failwith "invalid PartialGetMsgPayload call"
    | Resp.BulkString contents  ->  match contents with
                                    | BulkStrContents.Contents bs   -> bs
                                    | BulkStrContents.Nil           -> failwith "invalid PartialGetMsgPayload call"
    | Resp.Error bs             ->  bs
    | Resp.Integer _            ->  failwith "invalid PartialGetMsgPayload call"
    | Resp.SimpleString bs      ->  bs