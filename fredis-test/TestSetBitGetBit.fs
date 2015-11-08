module TestUtils

open Xunit
open FsCheck
open FsCheck.Xunit

open Swensen.Unquote


type BitIndexBytes = {BitIndex:int; Bs:byte array}

let genBitIndexBytes = 
    gen{
        let! arraySize = Gen.choose (1, 4096)
        let! bs = Gen.arrayOfLength arraySize Arb.generate<byte>
        let  maxBitIndex = (bs.Length * 8) - 1
        let!  bitIndex = Gen.choose(0, maxBitIndex)
        return {BitIndex = bitIndex; Bs = bs}
    }



type ArbOverrides() =
    static member BitIndexByteArray() = Arb.fromGen genBitIndexBytes



let private setBitReferenceImpl  bitVal bitIndex (bs:byte array) =
    let maxBitIdx = bs.Length * 8 - 1   // match redis behaviour, the bit index starts at the opposite end
    let revBitIndex = maxBitIdx - bitIndex
    let bitArray = System.Collections.BitArray(bs)
    bitArray.Set(revBitIndex, bitVal)
    let bs2 = Array.zeroCreate<byte>(bs.Length)
    bitArray.CopyTo(bs2, 0)
    bs2
    


let private getBitReferenceImpl  bitIndex (bs:byte array) =
    let maxBitIdx = bs.Length * 8 - 1   // match redis behaviour, the bit index starts at the opposite end
    let revBitIndex = maxBitIdx - bitIndex
    let bitArray = System.Collections.BitArray(bs)
    bitArray.Get revBitIndex



[< Property(Arbitrary = [| typeof<ArbOverrides> |]) >]
let ``GetBit matches reference implementation`` (idxBs:BitIndexBytes) =
    
    let bitIndex = idxBs.BitIndex
    let bs = idxBs.Bs

    test <@ Utils.GetBit bs bitIndex  = getBitReferenceImpl bitIndex bs @>
//    (Utils.GetBit bs bitIndex) = (getBitReferenceImpl bitIndex bs)



[< Property(Arbitrary = [| typeof<ArbOverrides> |]) >]
let ``SetBit matches reference implementation`` bitVal  (idxBs:BitIndexBytes) =

    let bitIndex = idxBs.BitIndex
    let bs = idxBs.Bs

    let bsCopy = Array.zeroCreate<byte>(bs.Length)
    bs.CopyTo(bsCopy, 0);
    Utils.SetBit bsCopy bitIndex bitVal

    let bsReference = setBitReferenceImpl bitVal bitIndex bs
    bsReference = bsCopy



[< Property(Arbitrary = [| typeof<ArbOverrides> |]) >]
let ``SetBit GetBit roundtrip agrees`` bitVal  (idxBs:BitIndexBytes) =

    let bitIndex = idxBs.BitIndex
    let bs = idxBs.Bs

    Utils.SetBit bs bitIndex bitVal

    let bitValOut = Utils.GetBit bs bitIndex
    bitVal = bitValOut 


[<Fact>]
let ``SetBit matches redis`` () =
    let bitIndex = 0
    let bs:byte array = [|49uy|]
    Utils.SetBit bs bitIndex true
    test <@ bs = [|0xb1uy|] @>