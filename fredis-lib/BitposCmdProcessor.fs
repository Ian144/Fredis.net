
module BitposCmdProcessor

open FredisTypes
open CmdCommon
open Utils



let private trueBitPosLookup  = [|-1; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 5; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 6; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 5; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 7; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 5; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 6; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 5; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0|]
let private falseBitPosLookup = [|0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 5; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 6; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 5; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 7; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 5; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 6; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 5; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; 4; 0; 1; 0; 2; 0; 1; 0; 3; 0; 1; 0; 2; 0; 1; 0; -1|]


let FindFirstBitIndex (lIndx:int) (uIndx:int) (searchVal:bool) (bs:byte []) : int =
    
    let bitPosLookup, funcx = 
            match searchVal with
            | true  -> trueBitPosLookup,    (fun indx -> bs.[indx] <> 0uy)
            | false -> falseBitPosLookup,   (fun indx -> bs.[indx] <> 255uy)

    let indxs = seq{lIndx..uIndx}

    let cFirstByteContainingBitValIndex = FSharpx.Choice.protect ( Seq.findIndex funcx )
    match cFirstByteContainingBitValIndex indxs with
    | Choice2Of2 _          -> -1   // indicating there are no bits of the value being searched for
    | Choice1Of2 byteIdx    ->  let firstFoundByte = bs.[byteIdx] |> int
                                byteIdx * 8 + bitPosLookup.[firstFoundByte]        


let Process key (bitVal:bool) (range:ArrayRange) (hashMap:HashMap) =
    let numSetBits =
            match (range, hashMap.ContainsKey(key)) with
            | (_, false)                                ->  -1L   // key not in map, so bitval is not found
                                                                    
            | (ArrayRange.All, true)                    ->  let bs = hashMap.[key]       
                                                            (FindFirstBitIndex 0 (bs.Length-1) bitVal bs) |> int64

            | (ArrayRange.Lower ll, true)               ->  let bs = hashMap.[key] 
                                                            let arrayUBound = bs.Length - 1
                                                            let uu = arrayUBound        
                                                            let optBounds = CmdCommon.RationaliseArrayBounds ll.Value uu arrayUBound //#### consider a function to only rationalise the lower bound here
                                                            match optBounds with
                                                            | Some (lower2, upper2) ->  (FindFirstBitIndex lower2 upper2 bitVal bs) |> int64  //#### is this conversion really neccessary
                                                            | None                  -> -1L


            | (ArrayRange.LowerUpper (ll,uu) , true)    ->  let bs = hashMap.[key] 
                                                            let arrayUBound = bs.Length - 1
                                                            let optBounds = CmdCommon.RationaliseArrayBounds ll.Value uu.Value arrayUBound 
                                                            match optBounds with
                                                            | Some (lower2, upper2) ->  (FindFirstBitIndex lower2 upper2 bitVal bs) |> int64  //#### is this conversion really neccessary
                                                            | None                  -> -1L

    Resp.Integer numSetBits
