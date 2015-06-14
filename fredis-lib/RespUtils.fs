[<RequireQualifiedAccess>]
module RespUtils


open FredisTypes

let errorBytes  = Utils.StrToBytes "-Error\r\n"






// #### remove the need for partial functions
let PartialGetMsgPayload respMsg = 
    match respMsg with
    | Resp.Array _           -> failwith "invalid PartialGetMsgPayload call"
    | Resp.BulkString bs     -> bs
    | Resp.Error bs          -> bs
    | Resp.Integer _         -> failwith "invalid PartialGetMsgPayload call"
    | Resp.SimpleString bs   -> bs