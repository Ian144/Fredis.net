
module FredisCmdProcessor

open FredisTypes
open Utils
open CmdCommon



   

  


let ExtendBytes (lenRequired:int) (bs:Bytes) = 
    if bs.Length >= lenRequired then
        bs
    else
        let bs2 = Array.zeroCreate<byte> lenRequired
        bs.CopyTo(bs2, 0)
        bs2


//#### consider replacing this with a hashmap of commands to handlers
let Execute (hashMap:HashMap) (cmd:FredisCmd) : Resp = 

    match cmd with
    | FredisCmd.Append (kk,vappend)         ->  match hashMap.ContainsKey(kk) with 
                                                | true  -> let val1 = hashMap.[kk]
                                                           let val2 = [|yield! val1; yield! vappend|]
                                                           hashMap.[kk] <- val2
                                                           Resp.Integer val2.LongLength
                                                | false -> hashMap.[kk] <- vappend
                                                           Resp.Integer vappend.LongLength

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
                                                Resp.SimpleString okBytes

    | FredisCmd.SetBit (key,offset,value)   ->  match hashMap.ContainsKey(key) with
                                                | true  ->  let lengthRequired = offset/8 + 1 
                                                            let bytes = hashMap.[key]
                                                            let bytes' = ExtendBytes lengthRequired bytes
                                                            let oldValue = GetBit bytes' offset
                                                            SetBit bytes' offset value
                                                            hashMap.[key] <- bytes'
                                                            match oldValue with 
                                                            | true  -> Resp.Integer 1L
                                                            | false -> Resp.Integer 0L

                                                | false ->  let numBytesRequired = offset/8 + 1 // if (offset%8) > 0 then 1 else 0
                                                            let bytes = Array.zeroCreate<byte> numBytesRequired
                                                            SetBit bytes offset value
                                                            hashMap.[key] <- bytes
                                                            Resp.Integer 0L // the 'old' value is considered to be zero if the key did not exist

    | FredisCmd.GetBit (key,offset)   ->        match hashMap.ContainsKey(key) with
                                                | true  ->  let lengthRequired = offset/8 + 1 
                                                            let bytes = hashMap.[key]
                                                            match lengthRequired > bytes.Length with
                                                            | true -> Resp.Integer 0L   // the value is past the end of the byte array, return zero
                                                            | false ->  let bitVal = GetBit bytes offset
                                                                        match bitVal with 
                                                                        | true  -> Resp.Integer 1L
                                                                        | false -> Resp.Integer 0L

                                                | false ->  Resp.Integer 0L // the 'old' value is considered to be zero if the key did not exist


    | FredisCmd.MSet kvPairs                ->  kvPairs |> List.iter (fun (kk,vv) -> hashMap.[kk] <- vv)
                                                Resp.SimpleString okBytes

    | FredisCmd.Get kk                      ->  match hashMap.ContainsKey(kk) with 
                                                | true  ->  hashMap.[kk] |> BulkStrContents.Contents |> Resp.BulkString
                                                | false ->  BulkStrContents.Nil |> Resp.BulkString

    | FredisCmd.GetSet (kk,newVal)          ->  match hashMap.ContainsKey(kk) with 
                                                | true  ->  let oldVal = hashMap.[kk]
                                                            hashMap.[kk] <- newVal
                                                            oldVal |> RespUtils.MakeBulkStr
                                                | false ->  hashMap.[kk] <- newVal
                                                            RespUtils.nilBulkStr


    | FredisCmd.Strlen kk                   ->  match hashMap.ContainsKey(kk) with 
                                                | true  ->  let len = hashMap.[kk].LongLength
                                                            Resp.Integer len              
                                                | false ->  Resp.Integer 0L

    | FredisCmd.MGet keys                   ->  let vals = 
                                                    keys |> List.map (fun kk -> 
                                                        match hashMap.ContainsKey(kk) with 
                                                        | true  ->  hashMap.[kk] |> RespUtils.MakeBulkStr
                                                        | false ->  RespUtils.nilBulkStr ) 
                                                vals |> List.toArray |> Resp.Array

    | FredisCmd.Ping                        ->  Resp.SimpleString pongBytes

    | FredisCmd.GetRange (key, range)       ->  match hashMap.ContainsKey(key) with 
                                                | false ->  RespUtils.nilBulkStr
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
                                                                                        (Array.sub bs lower1 count) |> RespUtils.MakeBulkStr
                                                            | None                  ->  RespUtils.nilBulkStr