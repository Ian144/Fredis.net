module TestFredisCmdsFSCheck


open System
open Xunit
open FsCheck
open FsCheck.Xunit

open CmdCommon
open FredisTypes



type Offsets = 
    static member Ints() =
        Arb.Default.Int32()
        |> Arb.filter (fun ii -> ii > 0 && ii < (pown 2 29) )






type PositiveInt32 = 
    static member Ints() =
        Arb.fromGen (Gen.choose(0, Int32.MaxValue))


// used to avoid creating very large byte arrays of length up to Int32.MaxValue in tests with an array or bit offset
type PositiveInt32SmallRange = 
    static member Ints() =
        Arb.fromGen (Gen.choose(0, 999))


//type MyPropertyAttribute() =
//    inherit PropertyAttribute (Arbitrary=[| typeof<PositiveInt32SmallRange> |])
//
//type ArbKey =
//    static member Create () = 
//        Arb.Default.String () |> Arb.filter (fun s -> not (String.IsNullOrEmpty s) && not (String.exists ((=) '\000') s)) 
//
//type ArbKey2 =
//    static member Create () = 
//        Arb.Default.NonEmptyString () |> Arb.convert string NonEmptyString

  

// if key is not in the hashmap initially, then the first setrange call will execute a different code path to the second
[< Property(Arbitrary = [| typeof<PositiveInt32SmallRange> |]) >]
let ``SETRANGE twice with same params is idempotent`` (key:Key) (value:byte []) (offset:int) = 
    let hashMap = HashMap()
    let cmd = FredisCmd.SetRange (key, offset, value)
    FredisCmdProcessor.Execute hashMap cmd |> ignore
    let valOut1 = hashMap.[key]
    FredisCmdProcessor.Execute hashMap cmd |> ignore
    let valOut2 = hashMap.[key]
    valOut1 = valOut2


[< Property(Arbitrary = [| typeof<PositiveInt32SmallRange> |]) >]
let ``SETRANGE when key does not exist, returns length of offset + length of value`` (key:Key) (value:byte []) (offset:int) = 
    let hashMap = HashMap()
    let cmd = FredisCmd.SetRange (key, offset, value)
    let actual = FredisCmdProcessor.Execute hashMap cmd 
    let expected = (offset + value.Length) |> int64 |> Resp.Integer
    expected = actual



// helper function to make ``GETRANGE SETRANGE round trip`` less ugly
let private GetBulkStrVal (resp:Resp) =
    match resp with
    | Resp.BulkString contents ->   match contents with 
                                    |BulkStrContents.Contents bs    -> bs
                                    |BulkStrContents.Nil            -> failwith "failed to get byte array from bulkString"
    | _ -> failwith "failed to get byte array from bulkString"
    

// if key is not in the hashmap initially, then the first setrange call will execute a different code path to the second
[< Property(Arbitrary = [| typeof<PositiveInt32SmallRange> |], Verbose = true) >]
let ``GETRANGE SETRANGE round trip`` (nesKey:NonEmptyString) (neValue:NonEmptyArray<byte>) (offset:int)  = 
    let valueIn = neValue.Get
    let skey = nesKey.Get
    let key = Key skey
    let hashMap = HashMap()
    let setRange = FredisCmd.SetRange (key, offset, valueIn)
    FredisCmdProcessor.Execute hashMap setRange |> ignore
    let optByteOffset = ByteOffset.create offset
    let byteOffset = optByteOffset.Value // its ok to assume optByteOffset is 'Some byteoffset' here, will throw and fail the test if this is not the case
    let getRange = FredisCmd.GetRange (key, ArrayRange.Lower byteOffset)
    let ret = FredisCmdProcessor.Execute hashMap getRange
    let valueOut = GetBulkStrVal ret
    valueOut = valueIn
    





// if the first offset is 0, and the key does not exist an array of 1 byte long will be created
// this gives a difference of "1 * 8 - 0 = 8"
//[<Property(Arbitrary = [| typeof<Offsets> |] )>]
//[<Property(Arbitrary = [| typeof<Offsets> |], Verbose=true)>]
//[<Property(Arbitrary = [| typeof<Offsets> |], MaxTest = 9999, Verbose=true, QuietOnSuccess = true)>]
[<  Property(Arbitrary = [| typeof<PositiveInt32> |]) >]
let ``SETBIT, array len created is never more than 8 longer than the bit offset`` (offset:int) =
    let key = Key "key"
    let value  = true
    let hashMap = HashMap()
    let cmd = FredisCmd.SetBit (key, offset, value)
    FredisCmdProcessor.Execute hashMap cmd |> ignore
    let createdArrayLen = hashMap.[key].Length
    let bitLenDiff = createdArrayLen * 8 - offset
    bitLenDiff >= 0 && bitLenDiff <= 8



let private int64ToBytes (ii:int64) =
    let ss = sprintf "%d" ii
    Utils.StrToBytes ss


let private BytesToInt64 (bs:Bytes) =
    bs |> Utils.BytesToStr |> System.Convert.ToInt64 




let private ReadRESPInteger (msg:Resp) = 
    match msg with
    | Resp.Integer ii   ->  ii
    | _                 ->  failwith "non integer RESP passed to ReadRESPInteger" 


let private CountSetBits (bs:Bytes) =
    let bitArray = System.Collections.BitArray(bs)
    let maxIndex = bitArray.Length - 1
    let mutable count = 0L
    for ctr in 0 .. maxIndex do
        if bitArray.[ctr] then
            count <- count + 1L
    count


// probably slow but probably correct reference implementation, to be used in property based testing
let private findFirstSetBitposReference (searchVal:bool) (bytes:byte []) =
    let ba = System.Collections.BitArray(bytes)
    let arr = Array.zeroCreate<bool>(ba.Length)
    
    for idx = 0 to ba.Length - 1 do
        arr.[idx] <- ba.Item(idx)

    match arr |> Array.exists (fun bl -> bl = searchVal) with
    | true -> arr |> Array.findIndex (fun bl -> bl = searchVal)
    | false -> -1


// probably slow but probably correct reference implementation, to be used in property based testing
let private findFirstSetBitposReferenceX (searchVal:bool) (startIdx:int) (endIdx:int) (bytes:byte []) =
    let ba = System.Collections.BitArray(bytes)
    let arr = Array.zeroCreate<bool>(ba.Length)
    
    let startIdx2 = if startIdx >= 0 then startIdx else 0
    let endIdx2 = if endIdx < arr.Length then endIdx else (arr.Length - 1)

    for idx = startIdx2 to endIdx2 do
        arr.[idx] <- ba.Item(idx)

    match arr |> Array.exists (fun bl -> bl = searchVal) with
    | true -> arr |> Array.findIndex (fun bl -> bl = searchVal)
    | false -> -1


// this test fails, 'OS X' redis returns 12, fredis returns 8 due to byte endianess
[<Fact>]
let ``Bitpos FindFirstBitIndex returns 12`` () =
    let bs = Array.zeroCreate<byte>(3)
    bs.[0] <- 0xFFuy
    bs.[1] <- 0xF0uy
    let uIndx = bs.GetUpperBound(0)
    <@BitposCmdProcessor.FindFirstBitIndex 0 uIndx false bs = findFirstSetBitposReference false bs@>

    

//[<Property(Verbose = true, MaxTest = 9999)>]
[<Property>]
let ``BitcountCmdProcessor.CountBitsInArrayRange agrees with alternate count method`` (bs:byte array) =
    BitcountCmdProcessor.CountSetBitsInRange (bs, 0, bs.Length) = CountSetBits bs



[<Property>]
let ``BitposCmdProcessor.FindFirstBitIndex true equals reference implementation`` (bs:byte array) =
    let uIndx = bs.GetUpperBound(0)
    BitposCmdProcessor.FindFirstBitIndex 0 uIndx true bs =  findFirstSetBitposReference true bs

[<Property>]
let ``BitposCmdProcessor.FindFirstBitIndex false equals reference implementation`` (bs:byte array) =
    let uIndx = bs.GetUpperBound(0)    
    BitposCmdProcessor.FindFirstBitIndex 0 uIndx false bs =  findFirstSetBitposReference false bs




[<Property( Arbitrary = [| typeof<PositiveInt32SmallRange> |] )>]
let ``SETBIT BITPOS roundtrip agree`` (offset:int) =
    let key = Key "key"
    let value  = true
    let hashMap = HashMap()
    let setCmd = FredisCmd.SetBit (key, offset, value)
    FredisCmdProcessor.Execute hashMap setCmd |> ignore

    let bitPosCmd = FredisCmd.Bitpos (key, value, ArrayRange.All)
    let ret = FredisCmdProcessor.Execute hashMap bitPosCmd 
    
    match ret with
    | Resp.Integer bitPosFound  -> (int bitPosFound) = offset
    | _                         -> false



[<Property( Arbitrary = [| typeof<PositiveInt32SmallRange> |] )>]
let ``SETBIT BITPOS roundtrip agree, set one bit to zero when all others are one`` (bitOffset:int) =
    let key = Key "key"
    let hashMap = HashMap()

    let arraySizeToCreate = (bitOffset / 8) * 2 + 1 // big enough to contain the offset

    let bs = Array.create<byte> arraySizeToCreate 255uy // create an array containing only 1's
    let setCmd = FredisCmd.Set (key, bs)
    FredisCmdProcessor.Execute hashMap setCmd |> ignore

    let setBitCmd = FredisCmd.SetBit (key, bitOffset, false)
    FredisCmdProcessor.Execute hashMap setBitCmd |> ignore

    let bitPosCmd = FredisCmd.Bitpos (key, false, ArrayRange.All)
    let ret = FredisCmdProcessor.Execute hashMap bitPosCmd 
    
    match ret with
    | Resp.Integer bitPosFound  -> (int bitPosFound) = bitOffset
    | _                         -> false



[<Property( Arbitrary = [| typeof<PositiveInt32SmallRange> |])>]
let ``SETBIT GETBIT roundtrip sets correct bit and nothing else`` (offset:int) =
    let key = Key "key"
    let value  = true
    let hashMap = HashMap()
    let setCmd = FredisCmd.SetBit (key, offset, value)
    FredisCmdProcessor.Execute hashMap setCmd |> ignore

    let getCmd = FredisCmd.GetBit (key, offset)
    let getBitValue = FredisCmdProcessor.Execute hashMap getCmd |> ReadRESPInteger 

    let bitcountCmd = FredisCmd.Bitcount (key, None)
    let bitCount = FredisCmdProcessor.Execute hashMap bitcountCmd |> ReadRESPInteger 
    
    1L = getBitValue && 1L = bitCount



[<Property>]
let ``INCRBY when key does not exist, value equals increment`` (increment:int64) =
    let hashMap = HashMap()
    let key = Key "key"
    let cmd = FredisCmd.IncrBy (key, increment)
    let _ = FredisCmdProcessor.Execute hashMap cmd
    let expected = int64ToBytes increment
    expected = hashMap.[key]



[<Property>]
let ``INCRBY when key does exist, value equals old + new`` (oldValue:int64) (increment:int64) =
    let hashMap = HashMap()
    let key = Key "key"
    let bsOldValue = int64ToBytes oldValue
    let setCmd = FredisCmd.Set (key, bsOldValue)
    let _ = FredisCmdProcessor.Execute hashMap setCmd
    let incrCmd = FredisCmd.IncrBy (key, increment)
    let _ = FredisCmdProcessor.Execute hashMap incrCmd
    let newValue = BytesToInt64 hashMap.[key]
    (oldValue+increment) = newValue




