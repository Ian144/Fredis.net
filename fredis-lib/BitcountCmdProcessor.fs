
module BitcountCmdProcessor

open FredisTypes
open CmdCommon
open Utils

let private bitCountLookup = [|0L; 1L; 1L; 2L; 1L; 2L; 2L; 3L; 1L; 2L; 2L; 3L; 2L; 3L; 3L; 4L; 1L; 2L; 2L; 3L; 2L; 3L; 3L; 4L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 1L; 2L; 2L; 3L; 2L; 3L; 3L; 4L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 1L; 2L; 2L; 3L; 2L; 3L; 3L; 4L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 4L; 5L; 5L; 6L; 5L; 6L; 6L; 7L; 1L; 2L; 2L; 3L; 2L; 3L; 3L; 4L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 4L; 5L; 5L; 6L; 5L; 6L; 6L; 7L; 2L; 3L; 3L; 4L; 3L; 4L; 4L; 5L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 4L; 5L; 5L; 6L; 5L; 6L; 6L; 7L; 3L; 4L; 4L; 5L; 4L; 5L; 5L; 6L; 4L; 5L; 5L; 6L; 5L; 6L; 6L; 7L; 4L; 5L; 5L; 6L; 5L; 6L; 6L; 7L; 5L; 6L; 6L; 7L; 6L; 7L; 7L; 8L|]




let CountSetBitsInRange ((arr:Bytes), (ll:int32), (uu:int32)) =
    let maxIndex = arr.Length - 1
    
    match arr, (ll, uu) with
    |[||], (_, _)                          ->   0L    // arr is empty
    | _, (ll1, _  )  when ll1 > maxIndex   ->   0L    // lower index > max
    | _, (_  , uu1)  when uu1 < 0          ->   0L    // upper index < 0
    | _, (ll1, uu1)  when ll1 > uu1        ->   0L    // lower > upper
    | arr, (ll1, uu1)                      ->   let ll2 = if (ll1 < 0) then 0 else ll1
                                                let uu2 = if (uu1 > maxIndex) then maxIndex else uu1
                                                let bitCounts = seq{    for ctr in ll2 .. uu2 do
                                                                        let bb = arr.[ctr]
                                                                        let index = int32 bb
                                                                        yield bitCountLookup.[index]    }
                                                Seq.sum bitCounts    







let Process key (optIntPair:optByteOffsetPair) (hashMap:HashMap) =
    match (optIntPair, hashMap.ContainsKey(key)) with
    | (_, false)              ->    Resp.Integer 0L                          // key not in map
                                                        
    | (None, true)            ->    let bs = hashMap.[key]       
                                    let ret = CountSetBitsInRange (bs, 0, (bs.Length-1)) // no bounds supplied, so consider all bytes
                                    Resp.Integer ret

    | (Some (ll,uu), true)    ->    let bs = hashMap.[key] 
                                    let arrayUBound = bs.GetUpperBound(0)
                                    let optBounds = CmdCommon.RationaliseArrayBounds ll.Value uu.Value arrayUBound
                                    match optBounds with
                                    | Some (lower2, upper2) -> CountSetBitsInRange (bs, lower2, upper2) |> Resp.Integer
                                    | None                  -> Resp.Integer 0L  
