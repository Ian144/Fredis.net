
module BitcountCmdProcessor

open FredisTypes
open CmdCommon
open Utils

let private bitCountLookup = [|0L; 1L; 1L; 2L; 1L; 2L; 2L; 3L; 1L; 2L; 2L; 3L; 2L; 3L; 3L; 4L; 1L; 2L; 2L; 3L; 2L; 3L; 3L; 4L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 1L; 2L; 2L; 3L; 2L; 3L; 3L; 4L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 1L; 2L; 2L; 3L; 2L; 3L; 3L; 4L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 4L; 5L; 5L; 6L; 5L; 6L; 6L; 7L; 1L; 2L; 2L; 3L; 2L; 3L; 3L; 4L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 4L; 5L; 5L; 6L; 5L; 6L; 6L; 7L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 4L; 5L; 5L; 6L; 5L; 6L; 6L; 7L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 4L; 5L; 5L; 6L; 5L; 6L; 6L; 7L; 4L; 5L; 5L; 6L; 5L; 6L; 6L; 7L; 5L; 6L; 6L; 7L; 6L; 7L; 7L; 8L|]




let CountSetBitsInRange ((arr:Bytes), (ll:int32), (uu:int32)) =
    let maxIndex = arr.Length - 1

    let ll2 = if ll > maxIndex then maxIndex else ll
    let ll3 = if ll2 < 0 then 0 else ll2
    let uu2 = if uu > maxIndex then maxIndex else uu
    let uu3 = if uu2 < 0 then 0 else uu2

    match arr with
    |[||]   ->  0L
    | _     ->  let bitCounts = seq{    for ctr in ll3 .. uu3 do
                                        let bb = arr.[ctr]
                                        let index = int32 bb
                                        yield bitCountLookup.[index]    }
                Seq.sum bitCounts






let Process key (optIntPair:optIntPair) (hashMap:HashMap) =
    let numSetBits =
            match (optIntPair, hashMap.ContainsKey(key)) with
            | (_, false)              ->    0L                          // key not in map
                                                        
            | (None, true)            ->    let bs = hashMap.[key]       
                                            CountSetBitsInRange (bs, 0, (bs.Length-1)) // no bounds supplied, so consider all bytes

            | (Some (ll,uu), true)    ->    let bs = hashMap.[key] 
                                            let arrayUBound = bs.Length - 1
                                            let lower2, upper2 = CmdCommon.RationaliseArrayBounds ll uu arrayUBound
                                            CountSetBitsInRange (bs, lower2, upper2)
    MakeRespIntegerArr numSetBits
