
module FredisCmdProcessor

open FredisTypes
open Utils

open CmdCommon



   

let RespStrLen (str:string) = MakeRespIntegerArr (int64 str.Length)
    


let ExtendBytes (lenRequired:int) (bs:Bytes) = 
    if bs.Length >= lenRequired then
        bs
    else
        let bs2 = Array.zeroCreate<byte> lenRequired
        bs.CopyTo(bs2, 0)
        bs2

//#### consider replacing this with a hashmap of commands to handlers
let Execute (hashMap:HashMap) (cmd:FredisCmd) : byte array = 
    match cmd with
    | FredisCmd.Append (kk,vappend)         ->  match hashMap.ContainsKey(kk) with 
                                                | true  -> let val1 = hashMap.[kk]
                                                           let val2 = [|yield! val1; yield! vappend|]
                                                           hashMap.[kk] <- val2
                                                           MakeRespIntegerArr val2.LongLength
                                                | false -> hashMap.[kk] <- vappend
                                                           MakeRespIntegerArr vappend.LongLength

    | FredisCmd.Bitcount (kk, optIntPair)   ->  BitcountCmdProcessor.Process kk optIntPair hashMap  
    
    | FredisCmd.BitOp op                    ->  BitopCmdProcessor.Process op hashMap
    
    | FredisCmd.Bitpos (key, bitVal, range) ->  BitposCmdProcessor.Process key bitVal range hashMap
          
    | FredisCmd.Decr kk                     ->  let increment = -1L
                                                CmdCommon.IncrementBy hashMap kk increment

    | FredisCmd.Incr kk                     ->  let increment = 1L
                                                CmdCommon.IncrementBy hashMap kk increment
    
    | FredisCmd.DecrBy (kk,decr)            ->  let increment = -1L * decr
                                                CmdCommon.IncrementBy hashMap kk increment

    | FredisCmd.IncrBy (kk,incr)            ->  CmdCommon.IncrementBy hashMap kk incr

    | FredisCmd.Set (kk,vv)                 ->  hashMap.[kk] <- vv
                                                okBytes

    | FredisCmd.SetBit (key,offset,value)   ->  match hashMap.ContainsKey(key) with
                                                | true  ->  let lengthRequired = offset/8 + 1 
                                                            let bytes = hashMap.[key]
                                                            let bytes' = ExtendBytes lengthRequired bytes
                                                            let oldValue = GetBit bytes' offset
                                                            SetBit bytes' offset value
                                                            hashMap.[key] <- bytes'
                                                            match oldValue with 
                                                            | true  -> Utils.MakeRespIntegerArr 1L
                                                            | false -> Utils.MakeRespIntegerArr 0L

                                                | false ->  let numBytesRequired = offset/8 + 1 // if (offset%8) > 0 then 1 else 0
                                                            let bytes = Array.zeroCreate<byte> numBytesRequired
                                                            SetBit bytes offset value
                                                            hashMap.[key] <- bytes
                                                            Utils.MakeRespIntegerArr 0L // the 'old' value is considered to be zero if the key did not exist

    | FredisCmd.GetBit (key,offset)   ->        match hashMap.ContainsKey(key) with
                                                | true  ->  let lengthRequired = offset/8 + 1 
                                                            let bytes = hashMap.[key]
                                                            match lengthRequired > bytes.Length with
                                                            | true -> Utils.MakeRespIntegerArr 0L   // the value is past the end of the byte array, return zero
                                                            | false ->  let bitVal = GetBit bytes offset
                                                                        match bitVal with 
                                                                        | true  -> Utils.MakeRespIntegerArr 1L
                                                                        | false -> Utils.MakeRespIntegerArr 0L

                                                | false ->  Utils.MakeRespIntegerArr 0L // the 'old' value is considered to be zero if the key did not exist


    | FredisCmd.MSet kvPairs                ->  kvPairs |> List.iter (fun (kk,vv) -> hashMap.[kk] <- vv)
                                                okBytes

    | FredisCmd.Get kk                      ->  match hashMap.ContainsKey(kk) with 
                                                | true  ->  let vv = hashMap.[kk] |> BytesToStr
                                                            MakeArraySingleRespBulkString vv |> StrToBytes
                                                | false ->  nilBytes

    | FredisCmd.GetSet (kk,newVal)          ->  match hashMap.ContainsKey(kk) with 
                                                | true  ->  let oldVal = hashMap.[kk] |> BytesToStr
                                                            hashMap.[kk] <- newVal
                                                            let ret = MakeArraySingleRespBulkString oldVal |> StrToBytes
                                                            ret
                                                | false ->  hashMap.[kk] <- newVal
                                                            nilBytes


    | FredisCmd.Strlen kk                   ->  match hashMap.ContainsKey(kk) with 
                                                | true  ->  let len = hashMap.[kk].LongLength
                                                            MakeRespIntegerArr len              
                                                | false ->  nilBytes

    | FredisCmd.MGet keys                   ->  let vals = 
                                                    keys |> List.map (fun kk -> 
                                                        match hashMap.ContainsKey(kk) with 
                                                        | true  ->  let vv = hashMap.[kk] |> BytesToStr
                                                                    MakeRespBulkString vv
                                                        | false ->  nilByteStr ) 
                                                let allValStr = String.concat "" vals
                                                (sprintf "*%d\r\n%s" vals.Length allValStr) |> StrToBytes

    | FredisCmd.Ping                        ->  pongBytes

    | FredisCmd.GetRange (key, range)       ->  match hashMap.ContainsKey(key) with 
                                                | false ->  emptyBytes
                                                | true  ->  let bs = hashMap.[key]
                                                            let upperBound = bs.GetUpperBound(0)
                                                            let lower,upper = 
                                                                    match range with
                                                                    | All                   -> 0,   upperBound
                                                                    | Lower ll              -> ll.Value,  upperBound
                                                                    | LowerUpper (ll, uu)   -> ll.Value,  uu.Value
                                                            
                                                            let optBounds = RationaliseArrayBounds lower upper upperBound
                                                            
                                                            match optBounds with
                                                            | Some (lower1, upper1) -> 
                                                                                        let count = (upper1 - lower1 + 1) // +1 because for example, when both lower and upper refer to the last element the count should be 1-
                                                                                        let bs2 = Array.sub bs lower1 count 
                                                                                        let retStr = bs2 |> BytesToStr
                                                                                        let ret = retStr |> MakeArraySingleRespBulkString |> StrToBytes
                                                                                        ret
                                                            | None                  ->  CmdCommon.emptyBytes