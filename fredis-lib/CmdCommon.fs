﻿
module CmdCommon

open FredisTypes
open Utils

// temporarily using ConcurrentDictionary in early development, just to have something that works from the multiple .net threadpool threads used by the async workflows + IOCP
// will experiment with other data structures at a later stage
type HashMap = System.Collections.Concurrent.ConcurrentDictionary<Key,Bytes>


let nilByteStr  = "$-1\r\n"
let errorBytes  = Utils.StrToBytes "-Error\r\n"
let pongBytes   = Utils.StrToBytes "+PONG\r\n"
let okBytes     = Utils.StrToBytes "+OK\r\n"
let nilBytes    = Utils.StrToBytes nilByteStr
let emptyBytes  = Utils.StrToBytes "+\r\n" 


// used by DECR, INCR, DECRBY and INCRBY
let IncrementBy (hashMap:HashMap) kk increment =
    match hashMap.ContainsKey(kk) with 
    | true  ->  let oVal = hashMap.[kk] |> BytesToStr |> FSharpx.FSharpOption.ParseInt64
                match oVal with
                | Some ii   ->  let newVal = ii + increment 
                                let bs = newVal |> System.Convert.ToString |> StrToBytes
                                hashMap.[kk] <- bs
                                Resp.Integer newVal
                | None      ->  Resp.Error ErrorMsgs.valueNotIntegerOrOutOfRange
                                                
    | false ->  let newVal = increment
                let bs = newVal |> System.Convert.ToString |> StrToBytes
                hashMap.[kk] <- bs
                Resp.Integer increment
    


    
// converts negative offsets to positive, see http://redis.io/commands/getrange
// ensures positive offsets are within array bounds and that lower <= upper
// for zero based arrays only
let RationaliseArrayBounds (ll:int) (uu:int) (uBound:int) = 

    let convertLowerToZeroBasedIndex idx =
        let idx1 = 
            match idx with
            | n when n >= 0 -> n

            // if n is negative then it is a downwards offset from the array's upper bound
            // -1 refers to the last element, hence the +1
            | n             ->  let zeroBasedIdx = uBound + n + 1   // convert to +ve
                                if (zeroBasedIdx < 0) then 0        // constrain to lower array bound
                                else zeroBasedIdx
        if (idx1 <= uBound) then 
            Some idx1
        else
            None // having a lower bound higher than uBound means nothing in the array is referenced

    let convertUpperToZeroBasedIndex idx =
        let idx1 = 
            match idx with
            | n when n >= 0 -> n

            // if n is negative then it is a downwards offset from the array's upper bound
            // -1 refers to the last element, hence the +1
            | n             ->  let zeroBasedIdx = uBound + n + 1           // convert to +vw
                                if (zeroBasedIdx > uBound) then uBound      // constrain to upper array bound
                                else zeroBasedIdx
        if (idx1 >= 0) then
            Some idx1
        else
            None      // having an upperIndex less that zero means nothing in the array is referenced

    let optlower1 = convertLowerToZeroBasedIndex ll
    let optUpper1 = convertUpperToZeroBasedIndex uu   

    match optlower1, optUpper1 with
    | None, _ | _, None     ->  None
    | Some ll1, Some uu1    ->  if (ll1 <= uu1) then    // lower and upper indices are ok, but lower must be <= upper for both to be valid together
                                    Some (ll1, uu1)
                                else
                                    None
