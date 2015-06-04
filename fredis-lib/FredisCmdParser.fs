
module FredisCmdParser

open FSharpx.Option
open FSharpx.Choice

open FredisTypes
open Utils








// extract the list of key-value pair mset params
let GetMSetParamPairs (msgArr:RESPMsg []) = 
    let msetParams = msgArr |> Array.toList |> List.tail |> List.map RespUtils.PartialGetMsgPayload // throw away the first element, which will be the string MSET
    let keys, vals = List.foldBack (fun x (l,r) -> x::r, l) msetParams ([],[]) // note the (l,r) -> (r,l) switch see: http://stackoverflow.com/questions/7942630/splitting-a-list-of-items-into-two-lists-of-odd-and-even-indexed-items
    List.zip keys vals |> List.map (fun (rawKey, vv) -> (BytesToKey rawKey), vv )



let Parse (msgArr:RESPMsg []) =
    let msgBytes    = RespUtils.PartialGetMsgPayload msgArr.[0]
    let msgStr      = BytesToStr(msgBytes)
    let arrLen      = Array.length msgArr

    //printfn "%A - %s" msgArr msgStr
    printfn "%s" msgStr

    //#### consider replacing this ever growing match statement with a map of string to function
    match msgStr.ToUpper() with
    | "APPEND" -> 
        match arrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let vv  = RespUtils.PartialGetMsgPayload msgArr.[2]
                    Choice1Of2 (FredisCmd.Append (kk, vv))
        | _     ->  Choice2Of2 ErrorMsgs.numArgsAppend
    
    | "BITCOUNT" -> 
        // up to 3 params - BITCOUNT key [startIndx endIndx],  the last two optional, but must be present together
        match arrLen with
        | 4 ->  let key         = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                let sLIdx       = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr 
                let sUIdx       = RespUtils.PartialGetMsgPayload msgArr.[3] |> BytesToStr
                let optParams   = maybe{    let! iLIndx = FSharpx.FSharpOption.ParseInt sLIdx
                                            let! iUIndx = FSharpx.FSharpOption.ParseInt sUIdx
                                            let! lIndx  = ByteOffset.create iLIndx  
                                            let! uIndx  = ByteOffset.create iUIndx
                                            return (key, Some(lIndx, uIndx)) }

                match optParams with
                | Some prms   ->  Choice1Of2 (FredisCmd.Bitcount prms)
                | None        ->  Choice2Of2 ErrorMsgs.valueNotIntegerOrOutOfRange
        | 2 ->  let key = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                Choice1Of2 (FredisCmd.Bitcount (key, None))
        | _ ->  Choice2Of2 ErrorMsgs.numArgsGet
    
    | "BITOP"   -> BitopCmdProcessor.Parse msgArr

    | "BITPOS" -> 
            // BITPOS key bit startByte endByte
            match arrLen with
            | 5 ->  
                    choose{ let key         = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                            let sBit        = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                            let! bit        = Utils.ParseChoiceBoolFromStr ErrorMsgs.badBitArgBitpos sBit
                            let sStartByte  = RespUtils.PartialGetMsgPayload msgArr.[3] |> BytesToStr
                            let! iStartByte = Utils.ParseChoiceInteger ErrorMsgs.valueNotIntegerOrOutOfRange sStartByte
                            let! startByte  = ByteOffset.createChoice iStartByte ErrorMsgs.valueNotIntegerOrOutOfRange
                            let sEndByte    = RespUtils.PartialGetMsgPayload msgArr.[4] |> BytesToStr
                            let! iEndByte   = Utils.ParseChoiceInteger ErrorMsgs.valueNotIntegerOrOutOfRange sEndByte
                            let! endByte    = ByteOffset.createChoice iEndByte ErrorMsgs.valueNotIntegerOrOutOfRange
                            let arrayRange  = ArrayRange.LowerUpper (startByte,endByte)
                            return FredisCmd.Bitpos (key, bit, arrayRange) }

            // BITPOS key bit startByte 
            | 4 ->  
                    choose{ let key             = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                            let strBit          = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                            let! bit            = Utils.ParseChoiceBoolFromStr ErrorMsgs.badBitArgBitpos strBit
                            let strStartByte    = RespUtils.PartialGetMsgPayload msgArr.[3] |> BytesToStr
                            let! startByte      = Utils.ParseChoiceInteger ErrorMsgs.valueNotIntegerOrOutOfRange strStartByte
                            let! startByte1     = ByteOffset.createChoice startByte ErrorMsgs.valueNotIntegerOrOutOfRange
                            let arrayRange      = ArrayRange.Lower startByte1
                            return FredisCmd.Bitpos (key, bit, arrayRange) }

            // BITPOS key bit
            | 3 ->  
                    choose{ let key  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                            let strBit = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                            let! bit = Utils.ParseChoiceBoolFromStr ErrorMsgs.badBitArgBitpos strBit
                            return FredisCmd.Bitpos (key, bit, ArrayRange.All) }



            | _ ->  Choice2Of2 ErrorMsgs.numArgsBitpos


    | "DECR" ->
        // should be "DECR key" only,so arrLen must be 2
        match arrLen with
        | 2     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    Choice1Of2 (FredisCmd.Decr kk)
        | _     ->  Choice2Of2 ErrorMsgs.numArgsDecr

    | "INCR" ->
        // should be "INCR key" only,so arrLen must be 2
        match arrLen with
        | 2     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    Choice1Of2 (FredisCmd.Incr kk)
        | _     ->  Choice2Of2 ErrorMsgs.numArgsIncr

    | "DECRBY" ->
        // should be "DECRBY key num" only,so arrLen must be 3
        match arrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let optCmd = RespUtils.PartialGetMsgPayload msgArr.[2] 
                                    |> BytesToStr
                                    |> FSharpx.FSharpOption.ParseInt64 
                                    |> Option.map (fun increment -> FredisCmd.DecrBy (kk,increment))
                    
                    match optCmd with
                    | Some cmd  ->  Choice1Of2 cmd
                    | None      ->  Choice2Of2 ErrorMsgs.valueNotIntegerOrOutOfRange
        | _     ->  Choice2Of2 ErrorMsgs.numArgsDecrBy

    | "INCRBY" ->
        // should be "INCRBY key" only,so arrLen must be 3
        match arrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let optCmd = RespUtils.PartialGetMsgPayload msgArr.[2] 
                                    |> BytesToStr
                                    |> FSharpx.FSharpOption.ParseInt64 
                                    |> Option.map (fun increment -> FredisCmd.IncrBy (kk,increment))
                    
                    match optCmd with
                    | Some cmd  ->  Choice1Of2 cmd
                    | None      ->  Choice2Of2 ErrorMsgs.valueNotIntegerOrOutOfRange

        | _     ->  Choice2Of2 ErrorMsgs.numArgsIncrBy
    
    | "SET" -> 
        match arrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let vv  = RespUtils.PartialGetMsgPayload msgArr.[2]
                    Choice1Of2 (FredisCmd.Set (kk, vv))
        | _     ->  Choice2Of2 ErrorMsgs.numArgsSet

    | "GETSET" -> 
        match arrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let vv  = RespUtils.PartialGetMsgPayload msgArr.[2]
                    Choice1Of2 (FredisCmd.GetSet (kk, vv))
        | _     ->  Choice2Of2 ErrorMsgs.numArgsGetSet


    | "GETRANGE" ->  // getrange key start end - no params are optional
        match arrLen with
        | 4     ->  choose{
                        let  key        = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                        let  sStartByte = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                        let! iStartByte = Utils.ParseChoiceInteger ErrorMsgs.valueNotIntegerOrOutOfRange sStartByte
                        let! startByte  = ByteOffset.createChoice iStartByte ErrorMsgs.valueNotIntegerOrOutOfRange
                        let  sEndByte   = RespUtils.PartialGetMsgPayload msgArr.[3] |> BytesToStr
                        let! iEndByte   = Utils.ParseChoiceInteger ErrorMsgs.valueNotIntegerOrOutOfRange sEndByte
                        let! endByte    = ByteOffset.createChoice iEndByte ErrorMsgs.valueNotIntegerOrOutOfRange
                        let  arrayRange = ArrayRange.LowerUpper (startByte,endByte)
                        return FredisCmd.GetRange (key, arrayRange)
                    }

        | _     ->  Choice2Of2 ErrorMsgs.numArgsGetRange




    | "SETBIT" -> 
        match arrLen with
        | 4     ->  choose{
                            let key  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                            let  offsetStr = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                            let! offset = Utils.ParseChoiceInteger ErrorMsgs.bitOffsetNotIntegerOrOutOfRange offsetStr 
                            let  bitValStr = RespUtils.PartialGetMsgPayload msgArr.[3] |> BytesToStr
                            let! intVal = Utils.ParseChoiceInteger ErrorMsgs.bitNotIntegerOrOutOfRange bitValStr
                            let! bitVal = Utils.ParseChoiceBoolFromInt ErrorMsgs.bitNotIntegerOrOutOfRange intVal
                            return FredisCmd.SetBit (key, offset, bitVal)
                        }

        | _     ->  Choice2Of2 ErrorMsgs.numArgsSetbit

    | "GETBIT" -> 
        match arrLen with
        | 3     ->  choose{
                            let key  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                            let  offsetStr = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                            let! offset = Utils.ParseChoiceInteger ErrorMsgs.bitOffsetNotIntegerOrOutOfRange offsetStr 
                            return FredisCmd.GetBit (key, offset)
                        }

        | _     ->  Choice2Of2 ErrorMsgs.numArgsGetbit


    | "MSET" -> 
        match (arrLen % 2) with         // mset will have an arbitrary number of key-value pairs, so including the cmd itself the array length must be odd
        | 1     ->  let paramPairs = GetMSetParamPairs msgArr
                    Choice1Of2 (FredisCmd.MSet paramPairs)
        | _     ->  Choice2Of2 ErrorMsgs.numArgsMSet

    | "GET" -> 
        match arrLen with
        | 2     ->  let kk = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    Choice1Of2 (FredisCmd.Get kk )
        | _     ->  Choice2Of2 ErrorMsgs.numArgsGet

    | "STRLEN" -> 
        match arrLen with
        | 2     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1]  |> BytesToKey
                    Choice1Of2 (FredisCmd.Strlen kk)
        | _     ->  Choice2Of2 ErrorMsgs.numArgsGet
    
    | "MGET" -> 
        match (arrLen > 1) with
        | true     ->   let keys = msgArr |> Array.toList |> List.tail |> List.map RespUtils.PartialGetMsgPayload |> List.map BytesToKey
                        Choice1Of2 (FredisCmd.MGet keys)
        | false    ->   Choice2Of2 ErrorMsgs.numArgsMGet

    | "PING"    ->  Choice1Of2 FredisCmd.Ping

    | _         ->  Choice2Of2 RespUtils.errorBytes   // unsupported or invalid redis command




let RespMsgToRedisCmds (respMsg:RESPMsg) =
    match respMsg with
    | RESPMsg.Array msgArray    -> Parse msgArray
    | RESPMsg.BulkString _      -> Choice2Of2 RespUtils.errorBytes 
    | RESPMsg.Error _           -> Choice2Of2 RespUtils.errorBytes
    | RESPMsg.Integer _         -> Choice2Of2 RespUtils.errorBytes
    | RESPMsg.SimpleString _    -> Choice2Of2 RespUtils.errorBytes