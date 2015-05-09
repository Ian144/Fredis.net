
module CmdCommon

open RESPTypes
open Utils

// temporarily using ConcurrentDictionary in early development, just to have something that works from the multiple .net threadpool threads used by the async workflows + IOCP
// will experiment with other data structures at a later stage
type HashMap = System.Collections.Concurrent.ConcurrentDictionary<string,Bytes>



let nilByteStr  = "$-1\r\n"
let errorBytes  = Utils.StrToBytes "-Error\r\n"
let pongBytes   = Utils.StrToBytes "+PONG\r\n"
let okBytes     = Utils.StrToBytes "+OK\r\n"
let nilBytes    = Utils.StrToBytes nilByteStr


// used by DECR, INCR, DECRBY and INCRBY
let IncrementBy (hashMap:HashMap) kk increment =
    match hashMap.ContainsKey(kk) with 
    | true  ->  let oVal = hashMap.[kk] |> BytesToStr |> FSharpx.FSharpOption.ParseInt64
                match oVal with
                | Some ii   ->  let newVal = ii + increment 
                                let bs = newVal |> System.Convert.ToString |> StrToBytes
                                hashMap.[kk] <- bs
                                MakeRespIntegerArr newVal
                | None      ->  ErrorMsgs.valueNotIntegerOrOutOfRange
                                                
    | false ->  let newVal = increment
                let bs = newVal |> System.Convert.ToString |> StrToBytes
                hashMap.[kk] <- bs
                MakeRespIntegerArr newVal
    


// convert negative indices based on array upper bound to be zero based
// constrain lower and upper bounds to array dimensions
// see redis GetRange cmd documenation
let RationaliseArrayBounds (lower:int) (upper:int) (arrayUBound) = 

    let convertToZeroBasedIndex = function
            | n when n >= 0 -> n
            | n             -> arrayUBound + n  // if n is negative then it is a downwards offset from the array's upper bound

    let constrainToArrayBounds =
                    function
                    |   n when n < 0            -> 0
                    |   n when n > arrayUBound  -> arrayUBound
                    |   n                       -> n 

    let lower1 = convertToZeroBasedIndex lower
    let upper1 = convertToZeroBasedIndex upper

    let lower2 = constrainToArrayBounds lower1
    let upper2 = constrainToArrayBounds upper1

    //#### lower2 can still be higher than upper2, should this be handled by the callers of this function?
    lower2, upper2
