[<RequireQualifiedAccess>]
module RespUtils


open FredisTypes

let errorBytes  = Utils.StrToBytes "-Error\r\n"






// #### remove the need for partial functions
let PartialGetMsgPayload respMsg = 
    match respMsg with
    | RESPMsg.Array _           -> failwith "invalid PartialGetMsgPayload call"
    | RESPMsg.BulkString bs     -> bs
    | RESPMsg.Error bs          -> bs
    | RESPMsg.Integer _         -> failwith "invalid PartialGetMsgPayload call"
    | RESPMsg.SimpleString bs   -> bs