
module CmdCommon

open FredisTypes
open Utils




type HashMap = System.Collections.Generic.Dictionary<Key,Bytes>





// simulate redis behaviour
// redis strict_Strtoll is not very strict, it will allow 
//    incr on a string such as "\x00DILMAUYZTHLMGUYS", i.e. when the first char is null zero
//    incr on -1\x00WKOPJXBVWAOIJNBV", when the first char is a digit
let private simulateStrict_Strtoll bs = 
   
    let isNotSpace (bb:byte) = 
        let chr = char bb
        System.Char.IsWhiteSpace chr |> not

    let firstIsNullZero = if Array.isEmpty bs then 
                            false
                          else
                            (bs.[0] = 0uy) || ( bs.[0] |> char |> System.Char.IsDigit)

    let noWhiteSpace = bs |> Array.forall isNotSpace

    let str = bs |> BytesToStr
    match firstIsNullZero, noWhiteSpace with
    | true, _  -> Some 0L
    | _, true  -> str |> FSharpx.FSharpOption.ParseInt64
    | _, false -> None



// used by DECR, INCR, DECRBY and INCRBY
let IncrementBy (hashMap:HashMap) kk increment =
    match hashMap.ContainsKey(kk) with 
    | true  ->  let bs = hashMap.[kk]
//                let oVal = simulateStrict_Strtoll bs
                let oVal =  bs |> BytesToStr |> FSharpx.FSharpOption.ParseInt64
                match oVal with
                | Some ii   ->  let newVal = ii + increment 
                                let bs2 = newVal |> System.Convert.ToString |> StrToBytes
                                hashMap.[kk] <- bs2
                                Resp.Integer newVal

                | None      ->  Resp.Error ErrorMsgs.valueNotIntegerOrOutOfRange
                                                
    | false ->  let newVal = increment
                let bs = newVal |> System.Convert.ToString |> StrToBytes
                hashMap.[kk] <- bs
                Resp.Integer increment
    

let IncrementByFloat (hashMap:HashMap) kk increment =
    match hashMap.ContainsKey(kk) with 
    | true  ->  let optVal = hashMap.[kk] |> BytesToStr |> FSharpx.FSharpOption.ParseDouble
                match optVal with
                | Some ff   ->  let newVal = ff + increment 
                                let bs = newVal |> System.Convert.ToString |> StrToBytes
                                hashMap.[kk] <- bs
                                Resp.BulkString (BulkStrContents.Contents bs)
                | None      ->  Resp.Error ErrorMsgs.valueNotAValidFloat
                                                
    | false ->  let newVal = increment
                let bs = newVal |> System.Convert.ToString |> StrToBytes
                hashMap.[kk] <- bs
                Resp.BulkString (BulkStrContents.Contents bs)




    
// converts negative offsets to positive, see http://redis.io/commands/getrange
// ensures positive offsets are within array bounds and that lower <= upper
let RationaliseArrayBounds (ll:int) (uu:int) (uBound:int) = 

    let strlen = uBound + 1

    // if n is negative then it is a downwards offset from the array's upper bound
    let convertNegIndex idx = if idx >=0 then idx else strlen + idx
    let constrainToZero idx = if idx < 0 then 0 else idx    // i'm not convinced this is what the setrange should do according to the command description, but its what redis does
    let funcx = convertNegIndex >> constrainToZero

    let ll1 = funcx ll
    let uu1 = funcx uu

    let ll2 = ll1
    let uu2 = if uu1 >= strlen then strlen - 1 else uu1

    match strlen, ll2, uu2 with
    | 0, _, _                       -> None
    | _, ll3, uu3 when ll3 > uu3    -> None
    | _, ll3, uu3                   -> Some (ll3, uu3)