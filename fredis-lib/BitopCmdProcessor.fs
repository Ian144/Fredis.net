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
                        
                        // three 'tails' to remove the cmd and bitop op, and destKey (TODO f# 4.0 might add 'skip' to List)
                        let srcKeys = msgArr |> Array.toList |>  List.tail |> List.tail |> List.tail |> List.map (RespUtils.PartialGetMsgPayload >> BytesToKey)

                        // there must be at least one srcKey as arrLen > 3, NOT requires exactly one src key
                        match operationStr.ToUpper(), srcKeys.Length with
                        | "AND", _  ->    let op = BitOpInner.AND (destKey, srcKeys.Head, srcKeys.Tail)
                                          Choice1Of2 (FredisCmd.BitOp op)
                        | "OR",  _  ->    let op = BitOpInner.OR (destKey, srcKeys.Head, srcKeys.Tail) 
                                          Choice1Of2 (FredisCmd.BitOp op)
                        | "XOR", _  ->    let op = BitOpInner.XOR (destKey, srcKeys.Head, srcKeys.Tail)
                                          Choice1Of2 (FredisCmd.BitOp op)
                        | "NOT", 1  ->    let op = BitOpInner.NOT (destKey, srcKeys.Head)
                                          Choice1Of2 (FredisCmd.BitOp op)
                        | "NOT", _  ->    Choice2Of2 ErrorMsgs.numKeysBitopNot
                        | _         ->    Choice2Of2 ErrorMsgs.syntaxError
    
    | _             ->  Choice2Of2 ErrorMsgs.numArgsBitop



    

let private getValOrEmpty (hashMap:HashMap) (key:Key) : Bytes = 
    match hashMap.ContainsKey(key) with
    | true  -> hashMap.[key]
    | false -> [||]



let private byteArrayBinOpAdaptor (binOp:byte -> byte -> byte) (bs:Bytes) (cs:Bytes) =
    let maxLen = max bs.Length cs.Length
    let maxIndex = maxLen - 1
    let dest:Bytes = Array.zeroCreate maxLen

    let getOrZero (xs:Bytes) idx = 
        match xs.Length > idx with
        | true  ->  xs.[idx]
        | false ->  0uy

    for idx = 0 to maxIndex do
        let b:byte = getOrZero bs idx
        let c:byte = getOrZero cs idx
        dest.[idx] <- binOp b c

    dest



let private byteArrayNot (bs:Bytes) =
    [|  for b in bs do
        yield (~~~) b   |]



let private byteArrayAnd = byteArrayBinOpAdaptor (&&&) 
let private byteArrayOr  = byteArrayBinOpAdaptor (|||) 
let private byteArrayXor = byteArrayBinOpAdaptor (^^^) 



let private applyBitOp (destKey:Key) (srcKey:Key) (srcKeysX:Key list) (hashMap:HashMap) (byteArrayBitOp:Bytes->Bytes->Bytes) = 
    let srcKeys = srcKey :: srcKeysX
    let anySrcKeyExists = srcKeys |> List.exists  (fun srcKey -> hashMap.ContainsKey(srcKey))
    match anySrcKeyExists with
    | true ->   let srcArrays = srcKeys |> List.map (getValOrEmpty hashMap) 
                let res = 
                    match srcArrays with
                    | [src] -> Array.copy src   // ensure that the result is a copy of src ([src] |> List.reduce returns the same array)
                    | _     -> srcArrays |> List.reduce byteArrayBitOp
                match res.Length with
                | 0 ->  hashMap.Remove destKey |> ignore
                        Resp.Integer  0L 
                | _ ->  hashMap.[destKey] <- res
                        Resp.Integer res.LongLength
    |false ->   hashMap.Remove(destKey) |> ignore
                Resp.Integer  0L 


let Process (op:BitOpInner) (hashMap:HashMap) =
    match op with
    | BitOpInner.AND (destKey, srcKey, srcKeys)     ->  applyBitOp destKey srcKey srcKeys hashMap byteArrayAnd
    | BitOpInner.OR  (destKey, srcKey, srcKeys)     ->  applyBitOp destKey srcKey srcKeys hashMap byteArrayOr
    | BitOpInner.XOR (destKey, srcKey, srcKeys)     ->  applyBitOp destKey srcKey srcKeys hashMap byteArrayXor
    | BitOpInner.NOT (destKey, srcKey)              ->  match hashMap.TryGetValue(srcKey) with
                                                        | true, vv  ->  match vv.LongLength with
                                                                        | 0L    ->  hashMap.Remove(destKey) |> ignore // matching redis behaviour
                                                                                    Resp.Integer 0L
                                                                        | _     ->  let res = byteArrayNot vv
                                                                                    hashMap.[destKey] <- res
                                                                                    Resp.Integer res.LongLength
                                                        | false, _  ->  hashMap.Remove(destKey) |> ignore
                                                                        Resp.Integer  0L

