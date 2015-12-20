module TestUtils

open Xunit
open FsCheck
open FsCheck.Xunit
open Swensen.Unquote

open FredisTypes


type BitIndexBytes = {BitIndex:int; Bs:byte array}

let genBitIndexBytes = 
    gen{
        let! arraySize = Gen.choose (1, 4096)
        let! bs = Gen.arrayOfLength arraySize Arb.generate<byte>
        let  maxBitIndex = (bs.Length * 8) - 1
        let!  bitIndex = Gen.choose(0, maxBitIndex)
        return {BitIndex = bitIndex; Bs = bs}
    }

type BitIndexBufferPair() =
    static member BitIndexByteArray() = Arb.fromGen genBitIndexBytes



type BitIdxBufSz = {BitIdx:int; BufSz:int}


let BitIdxBufSz = 
    gen{
        let! bufSz = Gen.choose (1, 4096)
        let  maxBitIndex = (bufSz * 8) - 1
        let!  bitIdx = Gen.choose(0, maxBitIndex)
        return {BitIdx = bitIdx; BufSz = bufSz}
    }


type BitIdxBufSzPair = 
    static member Pair() = Arb.fromGen BitIdxBufSz





// System.Collections.BitArray considers index 0 to be the LSB, redis considers it to be the MSB
// so cant use System.Collections.BitArray as a reference impl
//let private setBitReferenceImpl  bitVal bitIndex (bs:byte array) =
//    let bitArray = System.Collections.BitArray(bs)
//    bitArray.Set(bitIndex, bitVal)
//    let bs2 = Array.zeroCreate<byte>(bs.Length)
//    bitArray.CopyTo(bs2, 0)
//    bs2
//
//let private getBitReferenceImpl  bitIndex (bs:byte array) =
//    let bitArray = System.Collections.BitArray(bs)
//    bitArray.Get bitIndex
//
//[< Property(Arbitrary = [| typeof<ArbOverrides> |]) >]
//let ``GetBit matches reference implementation`` (idxBs:BitIndexBytes) =
//    
//    let bitIndex = idxBs.BitIndex
//    let bs = idxBs.Bs
//
//    test <@ Utils.GetBit bs bitIndex  = getBitReferenceImpl bitIndex bs @>
////    (Utils.GetBit bs bitIndex) = (getBitReferenceImpl bitIndex bs)
//
//
//
//[< Property(Arbitrary = [| typeof<ArbOverrides> |]) >]
//let ``SetBit matches reference implementation`` bitVal  (idxBs:BitIndexBytes) =
//
//    let bitIndex = idxBs.BitIndex
//    let bs = idxBs.Bs
//
//    let bsCopy = Array.zeroCreate<byte>(bs.Length)
//    bs.CopyTo(bsCopy, 0);
//    Utils.SetBit bsCopy bitIndex bitVal
//
//    let bsReference = setBitReferenceImpl bitVal bitIndex bs
//    bsReference = bsCopy



[< Property(Arbitrary = [| typeof<BitIdxBufSzPair> |], MaxTest = 9999) >]
let ``SetBit BitPos true roundtrip`` (bitIdxbufSz:BitIdxBufSz) =
    let bitIdx = bitIdxbufSz.BitIdx
    let bufSz = bitIdxbufSz.BufSz

    let bs = Array.zeroCreate<byte> bufSz
    Utils.SetBit bs bitIdx true
    let foundIdx = BitposCmdProcessor.FindFirstBitIndex 0 (bs.Length-1) true bs ArrayRange.All
    bitIdx = foundIdx



[< Property(Arbitrary = [| typeof<BitIdxBufSzPair> |], MaxTest = 9999) >]
let ``SetBit BitPos false roundtrip`` (bitIdxbufSz:BitIdxBufSz) =
    let bitIdx = bitIdxbufSz.BitIdx
    let bufSz = bitIdxbufSz.BufSz

    let bs = Array.create<byte> bufSz 255uy
    Utils.SetBit bs bitIdx false
    let foundIdx = BitposCmdProcessor.FindFirstBitIndex 0 (bs.Length-1) false bs ArrayRange.All
    bitIdx = foundIdx




[< Property(Arbitrary = [| typeof<BitIndexBufferPair> |]) >]
let ``SetBit GetBit roundtrip`` bitVal  (idxBs:BitIndexBytes) =

    let bitIndex = idxBs.BitIndex
    let bs = idxBs.Bs

    Utils.SetBit bs bitIndex bitVal

    let bitValOut = Utils.GetBit bs bitIndex
    bitVal = bitValOut 



[<Fact>]
let ``SetBit matches redis`` () =
    let bitIndex = 0
    let bs = [|49uy|]
    Utils.SetBit bs bitIndex true
    test <@ bs = [|177uy|] @>





