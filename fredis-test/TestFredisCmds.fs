﻿module TestFredisCmds


open NUnit.Framework
open Swensen.Unquote


open CmdCommon
open FredisTypes



let private ``byteOffset 0`` = (ByteOffset.create 0).Value
let private ``byteOffset 3`` = (ByteOffset.create 3).Value
let private ``byteOffset -1`` = (ByteOffset.create -1).Value
let private ``byteOffset -3`` = (ByteOffset.create -3).Value
let private ``range (0,3)`` = ArrayRange.LowerUpper (``byteOffset 0``, ``byteOffset 3``)
let private ``range(-3,-1)`` = ArrayRange.LowerUpper (``byteOffset -3``, ``byteOffset -1``)




[<TestFixture>]
type ``Execute GETRANGE`` () =
    let key = Key "key"
    let bs  = "This is a string" |> Utils.StrToBytes

    [<Test>]
    member this.``GETRANGE key start end returns empty string when key does not exist`` () = 
        let hashMap = HashMap()
        let cmd = FredisCmd.GetRange (key, ``range (0,3)``)
        test <@ CmdCommon.emptyBytes = FredisCmdProcessor.Execute hashMap cmd @>


    [<Test>]
    member this.``GETRANGE key 0 3 returns 'This' when key contains 'This is a string'`` () = 
        let hashMap = HashMap()
        let setCmd = FredisCmd.Set (key, bs)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let getRangeCmd = FredisCmd.GetRange (key, ``range (0,3)``)
        let expected = "This" |> Utils.MakeSingleArrayRespBulkString |> Utils.StrToBytes 
        test <@ expected = FredisCmdProcessor.Execute hashMap getRangeCmd @>


    [<Test>]
    member this.``GETRANGE key -3 -1 returns 'ing' when key contains 'This is a string'`` () = 
        let hashMap = HashMap()
        let setCmd = FredisCmd.Set (key, bs)
        let _ = FredisCmdProcessor.Execute hashMap setCmd

        let getRangeCmd = FredisCmd.GetRange (key, ``range(-3,-1)``)
        let expected = "ing" |> Utils.MakeSingleArrayRespBulkString |> Utils.StrToBytes 
        let ret = FredisCmdProcessor.Execute hashMap getRangeCmd
        test <@ expected = ret @>


    [<Test>]
    member this.``GETRANGE key upper bound < 0'`` () = 
        let hashMap = HashMap()
        let setCmd = FredisCmd.Set (key, bs)
        let _ = FredisCmdProcessor.Execute hashMap setCmd

        let getRangeCmd = FredisCmd.GetRange (key, ``range(-3,-1)``)
        let expected = "ing" |> Utils.MakeSingleArrayRespBulkString |> Utils.StrToBytes 
        let ret = FredisCmdProcessor.Execute hashMap getRangeCmd
        test <@ expected = ret @>






[<TestFixture>]
type ``Execute GETSET`` () =

    let key = Key "key"
    
    [<Test>]
    member this.``getset, new key sets value and returns nil``() =
        let hashMap = HashMap()
        let bsVal = (Utils.StrToBytes "val")
        let cmd = FredisCmd.GetSet (key, bsVal)
        let result = FredisCmdProcessor.Execute hashMap cmd
        test <@ bsVal = hashMap.[key] && CmdCommon.nilBytes = result @>


    [<Test>]
    member this.``getset, existing key sets new value and returns old``() =
        let hashMap = HashMap()
        let bsOldVal = (Utils.StrToBytes "oldVal")
        let strOldVal = Utils.MakeSingleArrayRespBulkString "oldVal"
        let bsNewVal = (Utils.StrToBytes "newVal")
        hashMap.[key] <- bsOldVal
        let cmd = FredisCmd.GetSet (key, bsNewVal)
        let result = FredisCmdProcessor.Execute hashMap cmd |> Utils.BytesToStr

        test <@ bsNewVal = hashMap.[key] && strOldVal = result @>





[<TestFixture>]
type ``Execute SET GET`` () =

    [<Test>]
    member this.``set get cmds, roundtrip``() =
        let hashMap = HashMap()
        let hkey = Key "key"
        let rawVal = "val"
        let hval = (Utils.StrToBytes rawVal)
        let setCmd = FredisCmd.Set (hkey, hval)
        let setResult = FredisCmdProcessor.Execute hashMap setCmd
        let getCmd = FredisCmd.Get hkey
        let getResult = FredisCmdProcessor.Execute hashMap getCmd
        let expectedBulkStrVal = Utils.MakeSingleArrayRespBulkString rawVal 
        let getResultStr = Utils.BytesToStr getResult
        test <@ setResult = CmdCommon.okBytes && expectedBulkStrVal = getResultStr @>


    [<Test>]
    member this.``get cmd, returns nil string when key does not exist``() =
        let hashMap = HashMap()
        let key = Key "key"
        let getCmd = FredisCmd.Get key
        let _ = FredisCmdProcessor.Execute hashMap getCmd 
        test <@ CmdCommon.nilByteStr = (FredisCmdProcessor.Execute hashMap getCmd |> Utils.BytesToStr) @>






[<TestFixture>]
type ``Execute SETBIT`` () =

    let key = Key "key"

    [<Test>]
    member this.``"SETBIT key 7 true" then "SETBIT key 8 false" creates byte array len 2`` () =
        let hashMap = HashMap()
        let _ = FredisCmdProcessor.Execute 
                    hashMap 
                    (FredisCmd.SetBit (key, 7, true))

        let _ = FredisCmdProcessor.Execute 
                    hashMap 
                    (FredisCmd.SetBit (key, 8, true))

        test <@ 2 = hashMap.[key].Length @>


    [<Test>]
    member this.``"SETBIT key 7 true" then "SETBIT key 7 false" returns 1 and resets to 0`` () =
        let hashMap = HashMap()
        let offset = 0
        let _ = FredisCmdProcessor.Execute 
                    hashMap 
                    (FredisCmd.SetBit (key, offset, true))

        let resetCmd = FredisCmd.SetBit (key, offset, false)
        test <@ Utils.MakeRespIntegerArr 1L = FredisCmdProcessor.Execute hashMap resetCmd &&
                [|0uy|] = hashMap.[key] @>




//replaced with fscheck property (fscheck.xunit tests)
//
//    [<Test>]
//    member this.``"SETBIT key 7 true",  when key does not exist returns integer 0`` () =
//        let hashMap = HashMap()
//        let key = Key "key"
//        let offset = 7
//        let value  = true
//        let cmd = FredisCmd.SetBit (key, offset, value)
//        test <@ Utils.MakeRespIntegerArr 0L = (FredisCmdProcessor.ExecuteRedisCmds hashMap cmd) @>
//
//
//    [<Test>]
//    member this.``"SETBIT key 0 true", byteArray created is one byte long`` () =
//        let hashMap = HashMap()
//        let key = Key "key"
//        let offset = 0
//        let value  = true
//        let cmd = FredisCmd.SetBit (key, offset, value)
//        FredisCmdProcessor.ExecuteRedisCmds hashMap cmd |> ignore
//        test <@ 1 = hashMap.[key].Length @>
//
//
//    [<Test>]
//    member this.``"SETBIT key 7 true", byteArray created is one byte long`` () =
//        let hashMap = HashMap()
//        let key = Key "key"
//        let offset = 7
//        let value  = true
//        let cmd = FredisCmd.SetBit (key, offset, value)
//        FredisCmdProcessor.ExecuteRedisCmds hashMap cmd |> ignore
//        test <@ 1 = hashMap.[key].Length @>
//
//
//    member this.``"SETBIT key 8 true", byteArray created is three bytes long`` () =
//        let hashMap = HashMap()
//        let key = Key "key"
//        let offset = 8
//        let value  = true
//        let cmd = FredisCmd.SetBit (key, offset, value)
//        FredisCmdProcessor.ExecuteRedisCmds hashMap cmd |> ignore
//        test <@ 1 = hashMap.[key].Length @>
//
//
//    [<Test>]
//    member this.``"SETBIT key 8 true", byteArray created is two bytes long`` () =
//        let hashMap = HashMap()
//        let key = Key "key"
//        let offset = 8
//        let value  = true
//        let cmd = FredisCmd.SetBit (key, offset, value)
//        FredisCmdProcessor.ExecuteRedisCmds hashMap cmd |> ignore
//        test <@ 2 = hashMap.[key].Length @>


[<TestFixture>]
type ``Execute INCRBY`` () =

//    [<Test>]
//    member this.``"INCRBY key 0" returns resp integer 0 when key does not exist`` () =
//        let hashMap = HashMap()
//        let cmd = FredisCmd.IncrBy ("key", 0L)
//        let actual = FredisCmdProcessor.Execute hashMap cmd 
//        let expected = Utils.MakeRespIntegerArr 0L
//        test <@ expected = actual @>
//        test <@ Utils.StrToBytes "0" = hashMap.["key"] @>
//
//
//    [<Test>]
//    member this.``"INCRBY key 89" returns resp integer 99 when key is "10"`` () =
//        let hashMap = HashMap()
//        let bVal = Utils.StrToBytes "10"
//        let key = Key "key"
//        let setCmd = FredisCmd.Set (key, bVal)
//        let _ = FredisCmdProcessor.Execute hashMap setCmd
//        let incrCmd = FredisCmd.IncrBy (key,89L)
//        test <@ Utils.MakeRespIntegerArr 99L = FredisCmdProcessor.Execute hashMap incrCmd @>
//
//
//    [<Test>]
//    member this.``"INCRBY key 99" returns resp integer 99 when key does not exist`` () =
//        let hashMap = HashMap()
//        let cmd = FredisCmd.IncrBy ("key", 99L)
//        test <@ Utils.MakeRespIntegerArr 99L = FredisCmdProcessor.Execute hashMap cmd @>
//        test <@ Utils.StrToBytes "99" = hashMap.["key"] @>


    [<Test>]
    member this.``"INCRBY key 99" returns error when value is "234293482390480948029348230948"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "234293482390480948029348230948"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let cmd = FredisCmd.IncrBy (key, 99L)
        test <@ ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap cmd) @>


    [<Test>]
    member this.``"INCRBY key 99"not an int" returns error`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "not an int"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let cmd = FredisCmd.IncrBy (key, 99L)
        test <@ ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap cmd) @>





[<TestFixture>]
type ``Execute INCR`` () =

    [<Test>]
    member this.``"INCR key" returns resp integer 11 when key is "10"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "10"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Incr key
        test <@ Utils.MakeRespIntegerArr 11L = FredisCmdProcessor.Execute hashMap decrCmd @>


    [<Test>]
    member this.``"INCR key" returns resp integer 1 when key does not exist`` () =
        let hashMap = HashMap()
        let key = Key "key"
        let decrCmd = FredisCmd.Incr key
        test <@ Utils.MakeRespIntegerArr 1L = FredisCmdProcessor.Execute hashMap decrCmd @>
        test <@ Utils.StrToBytes "1" = hashMap.[key] @>


    [<Test>]
    member this.``"INCR key" returns error when value is "234293482390480948029348230948"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "234293482390480948029348230948"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Incr key
        test <@ ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap decrCmd) @>


    [<Test>]
    member this.``"INCR key" returns error when value is "not an int"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "not an int"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Incr key
        test <@ ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap decrCmd) @>


[<TestFixture>]
type ``Execute DECR`` () =

    [<Test>]
    member this.``"DECR key" returns resp integer 9 when key is "10"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "10"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Decr key
        test <@ Utils.MakeRespIntegerArr 9L = FredisCmdProcessor.Execute hashMap decrCmd @>


    [<Test>]
    member this.``"DECR key" returns -1 when key does not exist`` () =
        let hashMap = HashMap()
        let key = Key "key"
        let decrCmd = FredisCmd.Decr key
        test <@ Utils.MakeRespIntegerArr -1L = FredisCmdProcessor.Execute hashMap decrCmd @>
        test <@ Utils.StrToBytes "-1" = hashMap.[key] @>


    [<Test>]
    member this.``"DECR key" returns error when value is "234293482390480948029348230948"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "234293482390480948029348230948"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Decr key
        test <@ ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap decrCmd) @>


    [<Test>]
    member this.``"DECR key" returns error when value is "not an int"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "not an int"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Decr key
        test <@ ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap decrCmd) @>




[<TestFixture>]
type ``Execute BITOP`` () =

    [<Test>]
    member this.``"BITOP AND destKey srcKey" returns resp integer 3 when val is "val"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "val"
        let srcKey = Key "srcKey"
        let destKey = Key "destKey"
        let setCmd = FredisCmd.Set (srcKey, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let boi = FredisTypes.BitOpInner.AND (destKey, [srcKey])
        let bitopCmd = FredisCmd.BitOp boi
        test <@ Utils.MakeRespIntegerArr 3L = FredisCmdProcessor.Execute hashMap bitopCmd @>



    [<Test>]
    member this.``"BITOP AND destKey srcKey1 srcKey2" result is length of longest value`` () =
        let hashMap = HashMap()
        let bVal1 = Utils.StrToBytes "abcdef"
        let bVal2 = Utils.StrToBytes "qwe"
        let destKey = Key "destKey"
        let srcKey1 = Key "srcKey1"
        let srcKey2 = Key "srcKey2"
        let set1 = FredisCmd.Set (srcKey1, bVal1)
        let _ = FredisCmdProcessor.Execute hashMap set1
        let set2 = FredisCmd.Set (srcKey2, bVal2)
        let _ = FredisCmdProcessor.Execute hashMap set2
        let boi = FredisTypes.BitOpInner.AND (destKey, [srcKey1; srcKey2])
        let bitopCmd = FredisCmd.BitOp boi
        test <@ Utils.MakeRespIntegerArr 6L = FredisCmdProcessor.Execute hashMap bitopCmd@>


    [<Test>]
    member this.``"BITOP AND destKey srcKey" srcKey not set, does not set destKey`` () =
        let hashMap = HashMap()
        let srcKey = Key "srcKey"
        let destKey = Key "destKey"
        let boi = FredisTypes.BitOpInner.AND (destKey, [srcKey])
        let bitopCmd = FredisCmd.BitOp boi
        let _ = FredisCmdProcessor.Execute hashMap bitopCmd
        let getCmd = FredisCmd.Get destKey
        let actual = FredisCmdProcessor.Execute hashMap getCmd |> Utils.BytesToStr
        test <@ not (hashMap.ContainsKey(destKey)) && (nilByteStr = actual) @>


    // may replace/augment the "... matches redis" tests with fscheck tests calling fredis and redis

    [<Test>]
    member this.``"BITOP AND destKey srcKey1 srcKey2" vals different lengths, result matches redis`` () =
        let hashMap = HashMap()
        let bVal1 = Utils.StrToBytes "abcdef"
        let bVal2 = Utils.StrToBytes "qwe"
        let destKey = Key "destKey"
        let srcKey1 = Key "srcKey1"
        let srcKey2 = Key "srcKey2"
        let set1 = FredisCmd.Set (srcKey1, bVal1)
        let _ = FredisCmdProcessor.Execute hashMap set1
        let set2 = FredisCmd.Set (srcKey2, bVal2)
        let _ = FredisCmdProcessor.Execute hashMap set2
        let boi = FredisTypes.BitOpInner.AND (destKey, [srcKey1; srcKey2])
        let bitopCmd = FredisCmd.BitOp boi
        let _ = FredisCmdProcessor.Execute hashMap bitopCmd
        let getCmd = FredisCmd.Get destKey
        let xx = Utils.BytesToStr [|0uy; 0uy; 0uy|]
        let yy = sprintf "aba%s" xx
        let expected = Utils.MakeSingleArrayRespBulkString yy // confirmed by trying this in redis
        test <@ expected =(FredisCmdProcessor.Execute hashMap getCmd |> Utils.BytesToStr) @>


    [<Test>]
    member this.``"BITOP OR destKey srcKey1 srcKey2" vals different lengths, result matches redis`` () =
        let hashMap = HashMap()
        let bVal1 = Utils.StrToBytes "abcdef"
        let bVal2 = Utils.StrToBytes "qwe"
        let destKey = Key "destKey"
        let srcKey1 = Key "srcKey1"
        let srcKey2 = Key "srcKey2"
        let set1 = FredisCmd.Set (srcKey1, bVal1)
        let _ = FredisCmdProcessor.Execute hashMap set1
        let set2 = FredisCmd.Set (srcKey2, bVal2)
        let _ = FredisCmdProcessor.Execute hashMap set2
        let boi = FredisTypes.BitOpInner.OR (destKey, [srcKey1; srcKey2])
        let bitopCmd = FredisCmd.BitOp boi
        let _ = FredisCmdProcessor.Execute hashMap bitopCmd
        let getCmd = FredisCmd.Get destKey
        let expected = Utils.MakeSingleArrayRespBulkString "qwgdef" // confirmed by trying this in redis
        test <@ expected = (FredisCmdProcessor.Execute hashMap getCmd |> Utils.BytesToStr) @>



    [<Test>]
    member this.``"BITOP XOR destKey srcKey1 srcKey2" vals different lengths, result matches redis`` () =
        let hashMap = HashMap()
        let destKey = Key "destKey"
        let srcKey1 = Key "srcKey1"
        let srcKey2 = Key "srcKey2"
        let bVal1 = Utils.StrToBytes "abcdef"
        let bVal2 = Utils.StrToBytes "qwe"
        let set1 = FredisCmd.Set (srcKey1, bVal1)
        let _ = FredisCmdProcessor.Execute hashMap set1
        let set2 = FredisCmd.Set (srcKey2, bVal2)
        let _ = FredisCmdProcessor.Execute hashMap set2
        let boi = FredisTypes.BitOpInner.XOR (destKey, [srcKey1; srcKey2])
        let bitopCmd = FredisCmd.BitOp boi
        let _ = FredisCmdProcessor.Execute hashMap bitopCmd
        let getCmd = FredisCmd.Get destKey
        let expectedBytes = Utils.BytesToStr [|16uy; 21uy; 6uy; 100uy; 101uy; 102uy|] // confirmed by trying this in redis
        let expected = Utils.MakeSingleArrayRespBulkString expectedBytes 
        test <@ expected = (FredisCmdProcessor.Execute hashMap getCmd |> Utils.BytesToStr) @>



    [<Test>]
    member this.``"BITOP NOT destKey srcKey" result matches redis`` () =
        let hashMap = HashMap()
        let destKey = Key "destKey"
        let srcKey  = Key "srcKey"
        let bVal = Utils.StrToBytes "abcdef" 
        let set = FredisCmd.Set (srcKey, bVal)
        let _ = FredisCmdProcessor.Execute hashMap set
        let boi = FredisTypes.BitOpInner.NOT (destKey, srcKey)
        let bitopCmd = FredisCmd.BitOp boi
        let _ = FredisCmdProcessor.Execute hashMap bitopCmd
        let getCmd = FredisCmd.Get destKey
        let expectedBytes = Utils.BytesToStr [|0x9euy; 0x9duy; 0x9cuy; 0x9buy; 0x9auy; 0x99uy|] // confirmed by trying this in redis
        let expected = Utils.MakeSingleArrayRespBulkString expectedBytes
        test <@ expected = (FredisCmdProcessor.Execute hashMap getCmd |> Utils.BytesToStr) @>



    [<Test>]
    member this.``"BITOP NOT destKey srcKey" srcKey does not exist, does not set destKey and returns (integer)0`` () =
        let hashMap = HashMap()
        let destKey = Key "destKey"
        let srcKey = Key "srcKey"
        let boi = FredisTypes.BitOpInner.NOT (destKey, srcKey)
        let bitopCmd = FredisCmd.BitOp boi
        let _ = FredisCmdProcessor.Execute hashMap bitopCmd |> Utils.BytesToInt64
        let getCmd = FredisCmd.Get destKey
        test <@ CmdCommon.nilByteStr = (FredisCmdProcessor.Execute hashMap getCmd |> Utils.BytesToStr) @>




    [<Test>]
    member this.``"BITOP AND destKey srcKey" destKey contains copy of srcKey val`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "val"
        let destKey = Key "destKey"
        let srcKey = Key "srcKey"
        let setCmd = FredisCmd.Set (srcKey, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let boi = FredisTypes.BitOpInner.AND (destKey, [srcKey])
        let bitopCmd = FredisCmd.BitOp boi
        let _ = FredisCmdProcessor.Execute hashMap bitopCmd
        let getCmd = FredisCmd.Get destKey
        test <@ Utils.MakeSingleArrayRespBulkString "val"  = (FredisCmdProcessor.Execute hashMap getCmd |> Utils.BytesToStr) @>




let StrToBulkStr = Utils.StrToBytes >> RESPMsg.BulkString



[<TestFixture>]
type ``Parse GETRANGE`` () =
    let getRange    = "GETRANGE"  |> StrToBulkStr
    let key         = "key"       |> StrToBulkStr
    let kkey        = Key "key"
    let startIdx    = "0"         |> StrToBulkStr
    let endIdx      = "3"         |> StrToBulkStr

    [<Test>]
    member this.``parse GETRANGE key start end returns FredisCmd.GetRange`` () = 
        let expected = FredisCmd.GetRange (kkey, ``range (0,3)``)
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [|getRange; key; startIdx; endIdx|] @>

    [<Test>]
    member this.``parse GETRANGE fails when no params supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsGetRange = FredisCmdParser.Parse [|getRange|] @>

    [<Test>]
    member this.``parse GETRANGE key fails when no 'start end' params supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsGetRange = FredisCmdParser.Parse [|getRange; key|] @>

    [<Test>]
    member this.``parse GETRANGE key start fails when no 'end' param supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsGetRange = FredisCmdParser.Parse [|getRange; key; startIdx|] @>







[<TestFixture>]
type ``Parse BITPOS`` () =
    let bitPos      = "BITPOS"  |> StrToBulkStr
    let key         = "key"     |> StrToBulkStr
    let kkey        = Key "key"
    let bitArg0     = "0"       |> StrToBulkStr
    let bitArg1     = "1"       |> StrToBulkStr
    let startByte   = "0"       |> StrToBulkStr
    let endByte     = "3"      |> StrToBulkStr

    let badStartByte   = "not an int"   |> StrToBulkStr
    let badEndByte     = "not an int"   |> StrToBulkStr
    let badBit         = "9"            |> StrToBulkStr




    //BITPOS key bit [start] [end]

    [<Test>]
    member this.``parse BITPOS fails when no params supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsBitpos = FredisCmdParser.Parse [|bitPos|] @>

    [<Test>]
    member this.``parse BITPOS key fails when no bit supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsBitpos = FredisCmdParser.Parse [|bitPos; key|] @>

    [<Test>]
    member this.``parse BITPOS key 9 fails as 9 is not convertable to a bit`` () = 
        test <@ Choice2Of2 ErrorMsgs.badBitArgBitpos = FredisCmdParser.Parse [|bitPos; key; badBit|] @>

    [<Test>]
    member this.``parse BITPOS key 0 succeeds`` () = 
        let expected = FredisCmd.Bitpos (kkey, false, ArrayRange.All)
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [|bitPos; key; bitArg0|] @>

    [<Test>]
    member this.``parse BITPOS key 1 succeeds`` () = 
        let expected = FredisCmd.Bitpos (kkey, true, ArrayRange.All)
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [|bitPos; key; bitArg1|] @>

    [<Test>]
    member this.``parse BITPOS key bitArg1 startByte succeeds`` () = 
        let nineByteOffset = (ByteOffset.create 9).Value
        let nineBlkStr   = "9" |> StrToBulkStr
        let expected = FredisCmd.Bitpos (kkey, true, ArrayRange.Lower nineByteOffset)
        let actual = FredisCmdParser.Parse [|bitPos; key; bitArg1; nineBlkStr|]
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [|bitPos; key; bitArg1; nineBlkStr|] @>

    [<Test>]
    member this.``parse BITPOS key bitArg1 startByte endByte succeeds`` () = 
        let expected = FredisCmd.Bitpos (kkey, true, ``range (0,3)``)
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [|bitPos; key; bitArg1; startByte; endByte|] @>

    [<Test>]
    member this.``parse BITPOS key true "not and int" fails with 'value not integer'`` () = 
        test <@ Choice2Of2 ErrorMsgs.valueNotIntegerOrOutOfRange = FredisCmdParser.Parse [|bitPos; key; bitArg1; badStartByte|] @>

    [<Test>]
    member this.``parse BITPOS key true 9 "not an int" fails with 'value not integer'`` () = 
        test <@ Choice2Of2 ErrorMsgs.valueNotIntegerOrOutOfRange = FredisCmdParser.Parse [|bitPos; key; bitArg1; startByte; badEndByte|] @>




[<TestFixture>]
type ``Parse INCRBY`` () =

    let incrBy   = "INCRBY"   |> StrToBulkStr
    let skey = "key" |> StrToBulkStr
    let kkey = Key "key"
    let increment = "9" |> StrToBulkStr

    [<Test>]
    member this.``parse INCRBY fails when no params supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsIncrBy = FredisCmdParser.Parse [|incrBy|] @>

    [<Test>]
    member this.``parse INCRBY fails when no increment supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsIncrBy = FredisCmdParser.Parse [|incrBy; skey|] @>

    [<Test>]
    member this.``parse INCRBY key incr succeeds`` () = 
        let expected = FredisCmd.IncrBy (kkey, 9L)
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [|incrBy; skey; increment |] @>






[<TestFixture>]
type ``Parse BITOP`` () =

    let bitop   = "BITOP"   |> StrToBulkStr
    let notOp   = "NOT"     |> StrToBulkStr
    let andOp   = "AND"     |> StrToBulkStr
    let orOp    = "OR"      |> StrToBulkStr
    let xorOp   = "XOR"     |> StrToBulkStr
    let destKey = "destKey" |> StrToBulkStr
    let kDestKey = Key "destKey"
    let srcKey1 = "srcKey1" |> StrToBulkStr
    let srcKey2 = "srcKey2" |> StrToBulkStr
    let srcKey3 = "srcKey3" |> StrToBulkStr
    let srcKey4 = "srcKey4" |> StrToBulkStr

    let kSrcKey1 = Key "srcKey1"
    let kSrcKey2 = Key "srcKey2"
    let kSrcKey3 = Key "srcKey3"
    let kSrcKey4 = Key "srcKey4"




    [<Test>]
    member this.``parse BITOP fails when no inner op supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsBitop = FredisCmdParser.Parse [|bitop|] @>

//  the unquote version of this test, above, requires less type annotation and has better compile time type safety
//    [<Test>]
//    member this.``parse BITOP fails when no inner op supplied fsunit`` () =
//        let expected:Choice<FredisCmd,byte []> = Choice2Of2 FredisErrorMsgs.numArgsBitop
//        expected |> should equal (BitopCmdProcessor.Parse [|bitop|])




    [<Test>]
    member this.``parse "BITOP NOT destKey" fails with missing source key`` () =
        test <@ Choice2Of2 ErrorMsgs.numArgsBitop = FredisCmdParser.Parse [| bitop; notOp; destKey |] @>



    [<Test>]
    member this.``parse "BITOP NOT destKey srcKey1 srcKey1" fails with num keys error`` () =
        test <@ Choice2Of2 ErrorMsgs.numKeysBitopNot = FredisCmdParser.Parse  [| bitop; notOp; destKey; srcKey1; srcKey2 |] @>



    [<Test>]
    member this.``parse "BITOP OR destKey srcKey" succeeds`` () =
        let expected = FredisCmd.BitOp (BitOpInner.OR (kDestKey,[kSrcKey1]))
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [| bitop; orOp; destKey; srcKey1 |] @>



    [<Test>]
    member this.``parse "BITOP NOT destKey srcKey" succeeds`` () =
        let expected = FredisCmd.BitOp (BitOpInner.NOT (kDestKey,kSrcKey1))
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [| bitop; notOp; destKey; srcKey1 |] @>



    [<Test>]
    member this.``parse "BITOP AND destKey" fails with num keys error`` () =
        test <@ Choice2Of2 ErrorMsgs.numArgsBitop = FredisCmdParser.Parse [| bitop; andOp; destKey |] @>



    [<Test>]
    member this.``parse "BITOP AND destKey srcKey" succeeds`` () =
        let expected = FredisCmd.BitOp (BitOpInner.AND (kDestKey,[kSrcKey1]))
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [| bitop; andOp; destKey; srcKey1 |] @>



    [<Test>]
    member this.``parse "BITOP AND destKey 4 src keys" succeeds`` () =
        let expected = FredisCmd.BitOp (BitOpInner.AND (kDestKey, [kSrcKey1; kSrcKey2; kSrcKey3; kSrcKey4]))
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [| bitop; andOp; destKey; srcKey1; srcKey2; srcKey3; srcKey4 |] @>


    [<Test>]
    member this.``parse "BITOP XOR destKey srcKey" succeeds`` () =
        let expected = FredisCmd.BitOp (BitOpInner.XOR (kDestKey,[kSrcKey1]))
        test <@ Choice1Of2 expected = FredisCmdParser.Parse [| bitop; xorOp; destKey; srcKey1 |] @>








