[<RequireQualifiedAccess>]
module BitopCmdProcessor

open System.Collections

open FSharpx.Option
open FredisTypes
open Utils
open CmdCommon






let Parse (msgArr:Resp []) =

    let arrLen = Array.length msgArr

    // must be at least BITOP OP DESTKEY SRCKEY1 in msgArr
    match arrLen with 
    | n when n > 3  ->  let operationStr   = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToStr
                        let destKey = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToKey
                        
                        // three 'tails' to remove the cmd and bitop op, and destKey (#### f# 4.0 might add 'skip' to List)
                        let srcKeys = msgArr |> Array.toList |>  List.tail |> List.tail |> List.tail |> List.map RespUtils.PartialGetMsgPayload |> List.map BytesToKey

                        // there must be at least one srcKey as arrLen > 3, NOT requires exactly one src key
                        match operationStr.ToUpper(), srcKeys.Length with
                        | "AND", _  ->    let op = BitOpInner.AND (destKey, srcKeys)
                                          Choice1Of2 (FredisCmd.BitOp op)
                        | "OR",  _  ->    let op = BitOpInner.OR (destKey, srcKeys) 
                                          Choice1Of2 (FredisCmd.BitOp op)
                        | "XOR", _  ->    let op = BitOpInner.XOR (destKey, srcKeys)
                                          Choice1Of2 (FredisCmd.BitOp op)
                        | "NOT", 1  ->    let op = BitOpInner.NOT (destKey, srcKeys.Head)
                                          Choice1Of2 (FredisCmd.BitOp op)
                        | "NOT", _  ->    Choice2Of2 ErrorMsgs.numKeysBitopNot
                        | _         ->    Choice2Of2 ErrorMsgs.syntaxError
    
    | _             ->  Choice2Of2 ErrorMsgs.numArgsBitop



    

let private GetValOrEmpty (hashMap:HashMap) (key:Key) : Bytes = 
    match hashMap.ContainsKey(key) with
    | true  -> hashMap.[key]
    | false -> [||]



let private ByteArrayBinOpAdaptor (binOp:byte -> byte -> byte) (bs:Bytes) (cs:Bytes) =
    let maxLen = max bs.Length cs.Length
    let maxIndex = maxLen - 1
    let dest:Bytes = Array.zeroCreate maxLen

    let GetOrZero (xs:Bytes) idx = 
        match xs.Length > idx with
        | true  ->  xs.[idx]
        | false ->  0uy

    for idx = 0 to maxIndex do
        let b:byte = GetOrZero bs idx
        let c:byte = GetOrZero cs idx
        dest.[idx] <- binOp b c

    dest



let private ByteArrayNot (bs:Bytes) =
    [|  for b in bs do
        yield (~~~) b   |]



let private ByteArrayAnd = ByteArrayBinOpAdaptor (&&&) 
let private ByteArrayOr  = ByteArrayBinOpAdaptor (|||) 
let private ByteArrayXor = ByteArrayBinOpAdaptor (^^^) 



let private applyBitOp (destKey:Key) (srcKeys:Key list) (hashMap:HashMap) (byteArrayBitOp:Bytes->Bytes->Bytes) = 
    let anySrcKeyExists = srcKeys |> List.exists  (fun srcKey -> hashMap.ContainsKey(srcKey))
    match anySrcKeyExists with
    | true ->
                let res = srcKeys |> List.map (GetValOrEmpty hashMap)   |> List.reduce (fun bs cs -> byteArrayBitOp bs cs)
                hashMap.[destKey] <- res
                Resp.Integer res.LongLength
    |false ->   Resp.Integer  0L 

let Process (op:BitOpInner) (hashMap:HashMap) =
    match op with
    | BitOpInner.AND (destKey, srcKeys)   ->    applyBitOp destKey srcKeys hashMap ByteArrayAnd
    | BitOpInner.OR  (destKey, srcKeys)   ->    applyBitOp destKey srcKeys hashMap ByteArrayOr
    | BitOpInner.XOR (destKey, srcKeys)   ->    applyBitOp destKey srcKeys hashMap ByteArrayXor
    | BitOpInner.NOT (destKey, srcKey)    ->    match hashMap.TryGetValue(srcKey) with
                                                | true, vv  ->  let res = ByteArrayNot vv
                                                                hashMap.[destKey] <- res
                                                                Resp.Integer res.LongLength
                                                | false, _  ->  Resp.Integer  0L

