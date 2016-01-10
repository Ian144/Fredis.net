
module FredisCmdParser

open FSharpx.Option
open FSharpx.Choice

open FredisTypes
open Utils







// TODO this is a partial function - fix this
// zeroth element contains mset
// odd indices are keys
// even indices are values
let private GetMSetParamPairs (msgArr:Resp []) = 
    let maxIdx = msgArr.Length - 1
    [   for idx in 2 .. 2 .. maxIdx do
        let keyIdx = idx - 1
        let keyBytes = RespUtils.PartialGetMsgPayload msgArr.[keyIdx]
        let key = BytesToKey keyBytes
        let vval = RespUtils.PartialGetMsgPayload msgArr.[idx]
        yield key, vval]




let ParseRESPtoFredisCmds (msgArr:Resp []) =
    let msgBytes    = RespUtils.PartialGetMsgPayload msgArr.[0]
    let msgStr      = BytesToStr(msgBytes)
    let msgArrLen   = Array.length msgArr

    //TODO consider replacing this ever growing match statement with a map of string to function
    match msgStr.ToUpper() with
    | "APPEND" -> 
        match msgArrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let vv  = RespUtils.PartialGetMsgPayload msgArr.[2]
                    Choice1Of2 (FredisCmd.Append (kk, vv))
        | _     ->  Choice2Of2 ErrorMsgs.numArgsAppend
    
    | "BITCOUNT" -> 
        // up to 3 params - BITCOUNT key [startIndx endIndx],  the last two optional, but must be present together
        match msgArrLen with
        | 4 ->  let key         = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                let sLIdx       = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr 
                let sUIdx       = RespUtils.PartialGetMsgPayload msgArr.[3] |> BytesToStr
                let optParams   = maybe{    let! iLIndx = FSharpx.FSharpOption.ParseInt sLIdx
                                            let! iUIndx = FSharpx.FSharpOption.ParseInt sUIdx
                                            let! lIndx  = ByteOffset.Create iLIndx
                                            let! uIndx  = ByteOffset.Create iUIndx
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
            match msgArrLen with
            | 5 ->  choose{ let key         = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                            let sBit        = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                            let! _          = Utils.ChoiceParseInt ErrorMsgs.valueNotIntegerOrOutOfRange sBit
                            let! bit        = Utils.ChoiceParseBool ErrorMsgs.badBitArgBitpos sBit
                            let sStartByte  = RespUtils.PartialGetMsgPayload msgArr.[3] |> BytesToStr
                            let! iStartByte = Utils.ChoiceParseInt ErrorMsgs.valueNotIntegerOrOutOfRange sStartByte
                            let! startByte  = ByteOffset.CreateChoice iStartByte ErrorMsgs.valueNotIntegerOrOutOfRange
                            let sEndByte    = RespUtils.PartialGetMsgPayload msgArr.[4] |> BytesToStr
                            let! iEndByte   = Utils.ChoiceParseInt ErrorMsgs.valueNotIntegerOrOutOfRange sEndByte
                            let! endByte    = ByteOffset.CreateChoice iEndByte ErrorMsgs.valueNotIntegerOrOutOfRange
                            let arrayRange  = ArrayRange.LowerUpper (startByte,endByte)
                            return FredisCmd.Bitpos (key, bit, arrayRange) }

            // BITPOS key bit startByte 
            | 4 ->  choose{ let key             = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                            let strBit          = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                            let! _ = Utils.ChoiceParseInt ErrorMsgs.valueNotIntegerOrOutOfRange strBit
                            let! bit            = Utils.ChoiceParseBool ErrorMsgs.badBitArgBitpos strBit
                            let strStartByte    = RespUtils.PartialGetMsgPayload msgArr.[3] |> BytesToStr
                            let! startByte      = Utils.ChoiceParseInt ErrorMsgs.valueNotIntegerOrOutOfRange strStartByte
                            let! startByte1     = ByteOffset.CreateChoice startByte ErrorMsgs.valueNotIntegerOrOutOfRange
                            let arrayRange      = ArrayRange.Lower startByte1
                            return FredisCmd.Bitpos (key, bit, arrayRange) }

            // BITPOS key bit
            | 3 ->  choose{ let key  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                            let strBit = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                            let! _ = Utils.ChoiceParseInt ErrorMsgs.valueNotIntegerOrOutOfRange strBit
                            let! bit = Utils.ChoiceParseBool ErrorMsgs.badBitArgBitpos strBit
                            return FredisCmd.Bitpos (key, bit, ArrayRange.All) }



            | _ ->  Choice2Of2 ErrorMsgs.numArgsBitpos


    | "DECR" ->
        // should be "DECR key" only,so arrLen must be 2
        match msgArrLen with
        | 2     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    Choice1Of2 (FredisCmd.Decr kk)
        | _     ->  Choice2Of2 ErrorMsgs.numArgsDecr

    | "INCR" ->
        // should be "INCR key" only,so arrLen must be 2
        match msgArrLen with
        | 2     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    Choice1Of2 (FredisCmd.Incr kk)
        | _     ->  Choice2Of2 ErrorMsgs.numArgsIncr

    | "DECRBY" ->
        // should be "DECRBY key num" only,so arrLen must be 3
        match msgArrLen with
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
        match msgArrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let optCmd = RespUtils.PartialGetMsgPayload msgArr.[2] 
                                    |> BytesToStr
                                    |> FSharpx.FSharpOption.ParseInt64 
                                    |> Option.map (fun increment -> FredisCmd.IncrBy (kk,increment))
                    
                    match optCmd with
                    | Some cmd  ->  Choice1Of2 cmd
                    | None      ->  Choice2Of2 ErrorMsgs.valueNotIntegerOrOutOfRange

        | _     ->  Choice2Of2 ErrorMsgs.numArgsIncrBy

    | "INCRBYFLOAT" ->
        match msgArrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let optCmd = RespUtils.PartialGetMsgPayload msgArr.[2] 
                                    |> BytesToStr
                                    |> FSharpx.FSharpOption.ParseDouble 
                                    |> Option.map (fun increment -> FredisCmd.IncrByFloat (kk,increment))
                    
                    match optCmd with
                    | Some cmd  ->  Choice1Of2 cmd
                    | None      ->  Choice2Of2 ErrorMsgs.valueNotAValidFloat

        | _     ->  Choice2Of2 ErrorMsgs.numArgsIncrBy

    
    | "SET" -> 
        match msgArrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let vv  = RespUtils.PartialGetMsgPayload msgArr.[2]
                    Choice1Of2 (FredisCmd.Set (kk, vv))
        | _     ->  Choice2Of2 ErrorMsgs.numArgsSet

    | "SETNX" -> 
        match msgArrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let vv  = RespUtils.PartialGetMsgPayload msgArr.[2]
                    Choice1Of2 (FredisCmd.SetNX (kk, vv))
        | _     ->  Choice2Of2 ErrorMsgs.numArgsSetNX

    | "GETSET" -> 
        match msgArrLen with
        | 3     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    let vv  = RespUtils.PartialGetMsgPayload msgArr.[2]
                    Choice1Of2 (FredisCmd.GetSet (kk, vv))
        | _     ->  Choice2Of2 ErrorMsgs.numArgsGetSet


    | "GETRANGE" ->  // getrange key start end - no params are optional
        match msgArrLen with
        | 4     ->  choose{
                        let  key        = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                        let  sStartByte = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                        let! iStartByte = Utils.ChoiceParseInt ErrorMsgs.valueNotIntegerOrOutOfRange sStartByte
                        let  sEndByte   = RespUtils.PartialGetMsgPayload msgArr.[3] |> BytesToStr
                        let! iEndByte   = Utils.ChoiceParseInt ErrorMsgs.valueNotIntegerOrOutOfRange sEndByte
                        return FredisCmd.GetRange (key, iStartByte, iEndByte)
                    }

        | _     ->  Choice2Of2 ErrorMsgs.numArgsGetRange


    | "SETRANGE" ->  // setrange key start bytes - no params are optional
        match msgArrLen with
        | 4     ->  choose{
                        let  key            = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                        let  sOffset        = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                        let! signedOffset   = Utils.ChoiceParsePosOrZeroInt ErrorMsgs.valueNotIntegerOrOutOfRange sOffset
                        let  offset         = uint32 signedOffset
                        let  bytes          = RespUtils.PartialGetMsgPayload msgArr.[3] 
                        let  totalLen       = signedOffset + bytes.Length
                        let! _              = ByteOffset.CreateChoice totalLen ErrorMsgs.valueNotIntegerOrOutOfRange //checks that an array longer than 512mb will not be needed
                        return FredisCmd.SetRange (key, offset, bytes)
                    }

        | _     ->  Choice2Of2 ErrorMsgs.numArgsSetRange

    | "SETBIT" -> 
        match msgArrLen with
        | 4     ->  choose{
                            let key  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                            let  offsetStr = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                            let! offset = Utils.ChoiceParseInt ErrorMsgs.bitOffsetNotIntegerOrOutOfRange offsetStr 
                            let uoffset = uint32 offset
                            let  bitValStr = RespUtils.PartialGetMsgPayload msgArr.[3] |> BytesToStr
                            let! intVal = Utils.ChoiceParseInt ErrorMsgs.bitNotIntegerOrOutOfRange bitValStr
                            let! bitVal = Utils.ChoiceParseBoolFromInt ErrorMsgs.bitNotIntegerOrOutOfRange intVal
                            return FredisCmd.SetBit (key, uoffset, bitVal)
                        }

        | _     ->  Choice2Of2 ErrorMsgs.numArgsSetbit

    | "GETBIT" -> 
        match msgArrLen with
        | 3     ->  choose{
                            let key  = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                            let  offsetStr = RespUtils.PartialGetMsgPayload msgArr.[2] |> BytesToStr
                            let! offset = Utils.ChoiceParsePosOrZeroInt ErrorMsgs.bitOffsetNotIntegerOrOutOfRange offsetStr 
                            let uoffset = uint32 offset
                            return FredisCmd.GetBit (key, uoffset)
                        }

        | _     ->  Choice2Of2 ErrorMsgs.numArgsGetbit

    | "MSET" -> 
        // mset will have an arbitrary number of key-value pairs, so including the cmd itself the array length must be odd
        // there must be at least one key value pair
        match (msgArrLen % 2) = 1 && msgArrLen >= 3 with
        | true  ->  let kvs = GetMSetParamPairs msgArr
                    Choice1Of2 (FredisCmd.MSet (kvs.Head, kvs.Tail))
        | false ->  Choice2Of2 ErrorMsgs.numArgsMSet

    | "MSETNX" -> 
        // msetNX will have an arbitrary number of key-value pairs, so including the cmd itself the array length must be odd
        // there must be at least one key value pair
        match (msgArrLen % 2) = 1 && msgArrLen >= 3 with 
        | true  ->  let kvs = GetMSetParamPairs msgArr
                    Choice1Of2 (FredisCmd.MSetNX (kvs.Head, kvs.Tail))
        | false ->  Choice2Of2 ErrorMsgs.numArgsMSetNX

    | "GET" -> 
        match msgArrLen with
        | 2     ->  let kk = RespUtils.PartialGetMsgPayload msgArr.[1] |> BytesToKey
                    Choice1Of2 (FredisCmd.Get kk )
        | _     ->  Choice2Of2 ErrorMsgs.numArgsGet

    | "STRLEN" -> 
        match msgArrLen with
        | 2     ->  let kk  = RespUtils.PartialGetMsgPayload msgArr.[1]  |> BytesToKey
                    Choice1Of2 (FredisCmd.Strlen kk)
        | _     ->  Choice2Of2 ErrorMsgs.numArgsGet
    
    | "MGET" -> 
        // mset will have an arbitrary number of keys, but there must be at least two array elements, so including the cmd itself
        match (msgArrLen >= 2) with
        | true      ->  let keys = msgArr |> Array.toList |> List.tail |> List.map (RespUtils.PartialGetMsgPayload >> BytesToKey)
                        Choice1Of2 (FredisCmd.MGet (keys.Head, keys.Tail)) // there must be at least one key, the rest are optional. An empty list of keys is an invalid state
        | false     ->  Choice2Of2 ErrorMsgs.numArgsMGet

    | "PING"        ->  Choice1Of2 FredisCmd.Ping

    | "FLUSHDB"     ->  Choice1Of2 FredisCmd.FlushDB

    | _             ->  Choice2Of2 ErrorMsgs.unknownCmd

    






let RespMsgToRedisCmds (respMsg:Resp) =
    match respMsg with
    | Resp.Array msgArray   -> ParseRESPtoFredisCmds msgArray
    |  _                    -> Choice2Of2 RespUtils.errorBytes 
