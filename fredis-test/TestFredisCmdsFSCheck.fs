module TestFredisCmdsFSCheck


open System
open Xunit
open FsCheck
open FsCheck.Xunit
open Swensen.Unquote

open CmdCommon
open FredisTypes





let private int64ToBytes (ii:int64) =
    let ss = sprintf "%d" ii
    Utils.StrToBytes ss

let private doubleToBytes (dd:double) =
    let ss = sprintf "%f" dd
    Utils.StrToBytes ss



let private BytesToInt64 (bs:Bytes) =
    bs |> Utils.BytesToStr |> System.Convert.ToInt64 

let private BytesToDouble (bs:Bytes) =
    bs |> Utils.BytesToStr |> System.Convert.ToDouble




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



type FloatRestrictedRange = 
    static member Floats() = 
        Arb.generate<float32>
        |> Gen.suchThat (fun ff -> (abs ff) < 999999999999.9f)
        |> Gen.map (fun ff -> float ff) 
        |> Arb.fromGen


let genNonEmptyBytes = 
    gen{
        let! arraySize = Gen.choose (1, 4096)
        let! bs = Gen.arrayOfLength arraySize Arb.generate<byte>
        return bs
    }





[<Property( Arbitrary = [| typeof<FloatRestrictedRange> |])>]
let ``INCRBYFLOAT when key does exist, value equals old + new`` (oldValue:float) (increment:float) =
    let hashMap = HashMap()
    let key = Key "key"
    let bsOldValue = doubleToBytes oldValue
    let setCmd = FredisCmd.Set (key, bsOldValue)
    let _ = FredisCmdProcessor.Execute hashMap setCmd
    let incrCmd = FredisCmd.IncrByFloat (key, increment)
    let _ = FredisCmdProcessor.Execute hashMap incrCmd
    let newValue = BytesToDouble hashMap.[key]
    let diff = abs (oldValue + increment - newValue)
    diff < 0.00001


[<Property( Arbitrary = [| typeof<FloatRestrictedRange> |])>]
let ``INCRBYFLOAT when key does not exist, value equals incr`` (increment:float) =
    let hashMap = HashMap()
    let key = Key "key"
    let incrCmd = FredisCmd.IncrByFloat (key, increment)
    FredisCmdProcessor.Execute hashMap incrCmd |> ignore
    let actual = BytesToDouble hashMap.[key]
    let diff = abs (increment - actual)
    diff < 0.000000001


type ArbOverrides() =
    static member NonEmptyByteArray() = Arb.fromGen genNonEmptyBytes
    static member Ints() = Arb.fromGen (Gen.choose(0, 999))


// if key is not in the hashmap initially, then the first setrange call will execute a different code path to the second
[< Property(Arbitrary = [| typeof<ArbOverrides> |]) >]
let ``SETRANGE twice with same params is idempotent`` (key:Key) (value:byte []) (offset:uint32) = 
    let hashMap = HashMap()
    let cmd = FredisCmd.SetRange (key, offset, value)
    FredisCmdProcessor.Execute hashMap cmd |> ignore
    let valOut1 = hashMap.[key]
    FredisCmdProcessor.Execute hashMap cmd |> ignore
    let valOut2 = hashMap.[key]
    valOut1 = valOut2


[< Property(Arbitrary = [| typeof<ArbOverrides> |]) >]
let ``SETRANGE when key does not exist, returns length of offset + length of value`` (key:Key) (value:byte []) (offset:uint32) = 
    let hashMap = HashMap()
    let cmd = FredisCmd.SetRange (key, offset, value)
    let actual = FredisCmdProcessor.Execute hashMap cmd 
    let sgnOffset = int offset
    let expected = (sgnOffset + value.Length) |> int64 |> Resp.Integer
    expected = actual


[< Property(Arbitrary = [| typeof<ArbOverrides> |]) >]
let ``SETRANGE when key does exist, returns length of offset + length of value`` (key:Key) (value:byte []) (offset:uint32) = 
    let hashMap = HashMap()
    let cmd1 = FredisCmd.SetRange (key, 0u, value)
    FredisCmdProcessor.Execute hashMap cmd1 |> ignore

    let cmd2 = FredisCmd.SetRange (key, offset, value)
    let actual = FredisCmdProcessor.Execute hashMap cmd2
    let sgnOffset = int offset
    let expected = (sgnOffset + value.Length) |> int64 |> Resp.Integer
    expected = actual




// helper function to make ``GETRANGE SETRANGE round trip`` less ugly
let private GetBulkStrVal (resp:Resp) =
    match resp with
    | Resp.BulkString contents ->   match contents with 
                                    |BulkStrContents.Contents bs    -> bs
                                    |BulkStrContents.Nil            -> failwith "failed to get byte array from bulkString"
    | _ -> failwith "failed to get byte array from bulkString"
    


[< Property(Arbitrary = [| typeof<PositiveInt32SmallRange> |]) >]
let ``SETRANGE GETRANGE round trip`` (nesKey:NonEmptyString) (neValue:NonEmptyArray<byte>) (offset:uint32)  = 
    let valueIn = neValue.Get
    let key = nesKey.Get |> Key
    let hashMap = HashMap()
    let setRange = FredisCmd.SetRange (key, offset, valueIn)
    FredisCmdProcessor.Execute hashMap setRange |> ignore

    let sgnOffset = int offset
    let lower = sgnOffset
    let upper = sgnOffset + valueIn.Length - 1 // if the range set was one byte then upper should equal lower, if two bytes then upper should be one more than lower
    let getRange = FredisCmd.GetRange (key, lower, upper)
    let ret = FredisCmdProcessor.Execute hashMap getRange
    let valueOut = GetBulkStrVal ret
    valueOut = valueIn




// if the first offset is 0, and the key does not exist an array of 1 byte long will be created
[<Property>]
let ``SETBIT, array len created is never more than 8 longer than the bit offset`` (offset:uint32) =
    let key = Key "key"
    let value  = true
    let hashMap = HashMap()
    let cmd = FredisCmd.SetBit (key, offset, value)
    FredisCmdProcessor.Execute hashMap cmd |> ignore
    let createdArrayLen = hashMap.[key].Length
    let bitLenDiff = createdArrayLen * 8 - (int offset)
    bitLenDiff >= 0 && bitLenDiff <= 8


let private ReadRESPInteger (msg:Resp) = 
    match msg with
    | Resp.Integer ii   ->  ii
    | _                 ->  failwith "non integer RESP passed to ReadRESPInteger" 


let private countSetBitsReferenceImpl (bs:Bytes) =
    let bitArray = System.Collections.BitArray(bs)
    let maxIndex = bitArray.Length - 1
    let mutable count = 0L
    for ctr in 0 .. maxIndex do
        if bitArray.[ctr] then
            count <- count + 1L
    count




[<Property(Verbose = true, MaxTest = 9999)>]
//[<Property>]
let ``BitcountCmdProcessor.CountBitsInArrayRange agrees with alternate count method`` (bs:byte array) =
    BitcountCmdProcessor.CountSetBitsInRange (bs, 0, bs.Length) = countSetBitsReferenceImpl bs



[<Property( Arbitrary = [| typeof<PositiveInt32SmallRange> |] )>]
let ``SETBIT BITPOS commands roundtrip`` (offset:uint32) =
    let key = Key "key"
    let value  = true
    let hashMap = HashMap()
    let setCmd = FredisCmd.SetBit (key, offset, value)
    FredisCmdProcessor.Execute hashMap setCmd |> ignore

    let bitPosCmd = FredisCmd.Bitpos (key, value, ArrayRange.All)
    let ret = FredisCmdProcessor.Execute hashMap bitPosCmd 
    
    match ret with
    | Resp.Integer bitPosFound  -> (int bitPosFound) = (int offset)
    | _                         -> false



[<Property>]
let ``SETBIT BITPOS roundtrip, set one bit to zero when all others are one`` (bitOffset:uint32) =
    let key = Key "key"
    let hashMap = HashMap()

    let arraySizeToCreate = (bitOffset / 8ul) * 2ul + 1ul // big enough to contain the offset

    let bs = Array.create<byte> (int arraySizeToCreate) 255uy // create an array containing only 1's
    let setCmd = FredisCmd.Set (key, bs)
    FredisCmdProcessor.Execute hashMap setCmd |> ignore

    let setBitCmd = FredisCmd.SetBit (key, bitOffset, false)
    FredisCmdProcessor.Execute hashMap setBitCmd |> ignore

    let bitPosCmd = FredisCmd.Bitpos (key, false, ArrayRange.All)
    let ret = FredisCmdProcessor.Execute hashMap bitPosCmd 
    
    match ret with
    | Resp.Integer bitPosFound  -> (int bitPosFound) = (int bitOffset)
    | _                         -> false



[<Property>]
let ``SETBIT GETBIT roundtrip sets correct bit and nothing else`` (offset:uint32) =
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

    

