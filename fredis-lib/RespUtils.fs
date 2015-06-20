[<RequireQualifiedAccess>]
module RespUtils


open FredisTypes


let errorBytes  = Utils.StrToBytes "-Error\r\n"

let RespStrLen (str:string) = Resp.Integer (int64 str.Length)

let MakeBulkStr = BulkStrContents.Contents >> Resp.BulkString

let nilBulkStr = Resp.BulkString BulkStrContents.Nil

let okSimpleStr = "OK" |> Utils.StrToBytes |> Resp.SimpleString

let pingSimpleStr = "PING" |> Utils.StrToBytes |> Resp.SimpleString
let pongSimpleStr = "PONG" |> Utils.StrToBytes |> Resp.SimpleString




// #### remove the need for partial functions
let PartialGetMsgPayload respMsg = 
    match respMsg with
    | Resp.Array _              ->  failwith "invalid PartialGetMsgPayload call"
    | Resp.BulkString contents  ->  match contents with
                                    | BulkStrContents.Contents bs   -> bs
                                    | BulkStrContents.Nil           -> failwith "invalid PartialGetMsgPayload call"
    | Resp.Error bs             ->  bs
    | Resp.Integer _            ->  failwith "invalid PartialGetMsgPayload call"
    | Resp.SimpleString bs      ->  bs