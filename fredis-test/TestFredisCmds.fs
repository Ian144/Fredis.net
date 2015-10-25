module TestFredisCmds


open Xunit
open Swensen.Unquote


open CmdCommon
open FredisTypes



let private ``byteOffset 0`` = (ByteOffset.create 0).Value
let private ``byteOffset 3`` = (ByteOffset.create 3).Value
let private ``byteOffset -1`` = (ByteOffset.create -1).Value
let private ``byteOffset -3`` = (ByteOffset.create -3).Value
let private ``range (0,3)`` = ArrayRange.LowerUpper (``byteOffset 0``, ``byteOffset 3``)
//let private ``range(-3,-1)`` = ArrayRange.LowerUpper (``byteOffset -3``, ``byteOffset -1``)


let private key = Key "key"
let private bs  = "This is a string" |> Utils.StrToBytes
let StrToBulkStr = Utils.StrToBytes >> RespUtils.MakeBulkStr

//let emptyBulkStr = Resp.BulkString [||] // i.e. an empty byte array

type ``Execute GETRANGE`` () =

    [<Fact>]
    static member ``GETRANGE key start end returns empty string when key does not exist`` () = 
        let hashMap = HashMap()
        let cmd = FredisCmd.GetRange (key, 0, 3)
        test <@ RespUtils.nilBulkStr = FredisCmdProcessor.Execute hashMap cmd @>


    [<Fact>]
    static member ``GETRANGE key 0 3 returns 'This' when key contains 'This is a string'`` () = 
        let hashMap = HashMap()
        let setCmd = FredisCmd.Set (key, bs)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let getRangeCmd = FredisCmd.GetRange (key, 0, 3)
        let expected = "This" |> StrToBulkStr
        test <@ expected = FredisCmdProcessor.Execute hashMap getRangeCmd @>


    [<Fact>]
    static member ``GETRANGE key -3 -1 returns 'ing' when key contains 'This is a string'`` () = 
        let hashMap = HashMap()
        let setCmd = FredisCmd.Set (key, bs)
        let _ = FredisCmdProcessor.Execute hashMap setCmd

        let getRangeCmd = FredisCmd.GetRange (key, -3, -1)
        let expected = "ing" |> StrToBulkStr
        let ret = FredisCmdProcessor.Execute hashMap getRangeCmd
        test <@ expected = ret @>


    [<Fact>]
    static member ``GETRANGE key upper bound < 0'`` () = 
        let hashMap = HashMap()
        let setCmd = FredisCmd.Set (key, bs)
        let _ = FredisCmdProcessor.Execute hashMap setCmd

        let getRangeCmd = FredisCmd.GetRange (key, -3, -1)
        let expected = "ing"|>  StrToBulkStr
        let ret = FredisCmdProcessor.Execute hashMap getRangeCmd
        test <@ expected = ret @>






type ``Execute GETSET`` () =

    [<Fact>]
    static member ``getset, new key sets value and returns nil``() =
        let hashMap = HashMap()
        let bsVal = (Utils.StrToBytes "val")
        let cmd = FredisCmd.GetSet (key, bsVal)
        let result = FredisCmdProcessor.Execute hashMap cmd
        test <@ bsVal = hashMap.[key] && RespUtils.nilBulkStr = result @>


    [<Fact>]
    static member ``getset, existing key sets new value and returns old``() =
        let hashMap = HashMap()
        let bsOldVal = "oldVal"     |> Utils.StrToBytes 
        let strOldVal = bsOldVal    |> RespUtils.MakeBulkStr
        let bsNewVal = "newVal"     |> Utils.StrToBytes
        hashMap.[key] <- bsOldVal
        let cmd = FredisCmd.GetSet (key, bsNewVal)
        let result = FredisCmdProcessor.Execute hashMap cmd 
        test <@ bsNewVal = hashMap.[key] && strOldVal = result @>





type ``Execute SET GET`` () =

    [<Fact>]
    static member ``set get cmds, roundtrip``() =
        let hashMap = HashMap()
        let hkey = Key "key"
        let rawVal = "val"
        let hval = (Utils.StrToBytes rawVal)
        let setCmd = FredisCmd.Set (hkey, hval)
        let setResult = FredisCmdProcessor.Execute hashMap setCmd
        let getCmd = FredisCmd.Get hkey
        let getResult = FredisCmdProcessor.Execute hashMap getCmd
        let expected =  rawVal |> StrToBulkStr
        test <@ setResult = RespUtils.okSimpleStr && expected = getResult @>



    [<Fact>]
    static member ``get cmd, returns nil string when key does not exist``() =
        let hashMap = HashMap()
        let key = Key "key"
        let getCmd = FredisCmd.Get key
        test <@ RespUtils.nilBulkStr = FredisCmdProcessor.Execute hashMap getCmd @>






type ``Execute SETBIT`` () =

    [<Fact>]
    static member ``"SETBIT key 7 true" then "SETBIT key 8 false" creates byte array len 2`` () =
        let hashMap = HashMap()
        let _ = FredisCmdProcessor.Execute 
                    hashMap 
                    (FredisCmd.SetBit (key, 7, true))

        let _ = FredisCmdProcessor.Execute 
                    hashMap 
                    (FredisCmd.SetBit (key, 8, true))

        test <@ 2 = hashMap.[key].Length @>


    [<Fact>]
    static member ``"SETBIT key 7 true" then "SETBIT key 7 false" returns 1 and resets to 0`` () =
        let hashMap = HashMap()
        let offset = 0
        let _ = FredisCmdProcessor.Execute 
                    hashMap 
                    (FredisCmd.SetBit (key, offset, true))

        let resetCmd = FredisCmd.SetBit (key, offset, false)
        test <@ Resp.Integer 1L = FredisCmdProcessor.Execute hashMap resetCmd &&
                [|0uy|] = hashMap.[key] @>



type ``Execute INCRBY`` () =

//    [<Fact>]
//    static member ``"INCRBY key 0" returns resp integer 0 when key does not exist`` () =
//        let hashMap = HashMap()
//        let cmd = FredisCmd.IncrBy ("key", 0L)
//        let actual = FredisCmdProcessor.Execute hashMap cmd 
//        let expected = Resp.Integer 0L
//        test <@ expected = actual @>
//        test <@ Utils.StrToBytes "0" = hashMap.["key"] @>
//
//
//    [<Fact>]
//    static member ``"INCRBY key 89" returns resp integer 99 when key is "10"`` () =
//        let hashMap = HashMap()
//        let bVal = Utils.StrToBytes "10"
//        let key = Key "key"
//        let setCmd = FredisCmd.Set (key, bVal)
//        let _ = FredisCmdProcessor.Execute hashMap setCmd
//        let incrCmd = FredisCmd.IncrBy (key,89L)
//        test <@ Resp.Integer 99L = FredisCmdProcessor.Execute hashMap incrCmd @>
//
//
//    [<Fact>]
//    static member ``"INCRBY key 99" returns resp integer 99 when key does not exist`` () =
//        let hashMap = HashMap()
//        let cmd = FredisCmd.IncrBy ("key", 99L)
//        test <@ Resp.Integer 99L = FredisCmdProcessor.Execute hashMap cmd @>
//        test <@ Utils.StrToBytes "99" = hashMap.["key"] @>


    [<Fact>]
    static member ``"INCRBY key 99" returns error when value is "234293482390480948029348230948"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "234293482390480948029348230948"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let cmd = FredisCmd.IncrBy (key, 99L)
        test <@ Resp.Error ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap cmd) @>


    [<Fact>]
    static member ``"INCRBY key 99"not an int" returns error`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "not an int"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let cmd = FredisCmd.IncrBy (key, 99L)
        test <@ Resp.Error ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap cmd) @>




type ``Execute INCR`` () =

    [<Fact>]
    static member ``"INCR key" returns resp integer 11 when key is "10"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "10"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Incr key
        test <@ Resp.Integer 11L = FredisCmdProcessor.Execute hashMap decrCmd @>


    [<Fact>]
    static member ``"INCR key" returns resp integer 1 when key does not exist`` () =
        let hashMap = HashMap()
        let key = Key "key"
        let decrCmd = FredisCmd.Incr key
        test <@ Resp.Integer 1L = FredisCmdProcessor.Execute hashMap decrCmd @>
        test <@ Utils.StrToBytes "1" = hashMap.[key] @>


    [<Fact>]
    static member ``"INCR key" returns error when value is "234293482390480948029348230948"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "234293482390480948029348230948"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Incr key
        test <@ Resp.Error ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap decrCmd) @>


    [<Fact>]
    static member ``"INCR key" returns error when value is "not an int"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "not an int"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Incr key
        test <@ Resp.Error ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap decrCmd) @>


type ``Execute DECR`` () =

    [<Fact>]
    static member ``"DECR key" returns resp integer 9 when key is "10"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "10"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Decr key
        test <@ Resp.Integer 9L = FredisCmdProcessor.Execute hashMap decrCmd @>


    [<Fact>]
    static member ``"DECR key" returns -1 when key does not exist`` () =
        let hashMap = HashMap()
        let key = Key "key"
        let decrCmd = FredisCmd.Decr key
        test <@ Resp.Integer -1L = FredisCmdProcessor.Execute hashMap decrCmd @>
        test <@ Utils.StrToBytes "-1" = hashMap.[key] @>


    [<Fact>]
    static member ``"DECR key" returns error when value is "234293482390480948029348230948"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "234293482390480948029348230948"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Decr key
        test <@ Resp.Error ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap decrCmd) @>


    [<Fact>]
    static member ``"DECR key" returns error when value is "not an int"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "not an int"
        let key = Key "key"
        let setCmd = FredisCmd.Set (key, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let decrCmd = FredisCmd.Decr key
        test <@ Resp.Error ErrorMsgs.valueNotIntegerOrOutOfRange = (FredisCmdProcessor.Execute hashMap decrCmd) @>




type ``Execute BITOP`` () =

    [<Fact>]
    static member ``"BITOP AND destKey srcKey" returns resp integer 3 when val is "val"`` () =
        let hashMap = HashMap()
        let bVal = Utils.StrToBytes "val"
        let srcKey = Key "srcKey"
        let destKey = Key "destKey"
        let setCmd = FredisCmd.Set (srcKey, bVal)
        let _ = FredisCmdProcessor.Execute hashMap setCmd
        let boi = FredisTypes.BitOpInner.AND (destKey, [srcKey])
        let bitopCmd = FredisCmd.BitOp boi
        test <@ Resp.Integer 3L = FredisCmdProcessor.Execute hashMap bitopCmd @>



    [<Fact>]
    static member ``"BITOP AND destKey srcKey1 srcKey2" result is length of longest value`` () =
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
        test <@ Resp.Integer 6L = FredisCmdProcessor.Execute hashMap bitopCmd@>


    [<Fact>]
    static member ``"BITOP AND destKey srcKey" srcKey not set, does not set destKey`` () =
        let hashMap = HashMap()
        let srcKey = Key "srcKey"
        let destKey = Key "destKey"
        let boi = FredisTypes.BitOpInner.AND (destKey, [srcKey])
        let bitopCmd = FredisCmd.BitOp boi
        let _ = FredisCmdProcessor.Execute hashMap bitopCmd
        let getCmd = FredisCmd.Get destKey
        let actual = FredisCmdProcessor.Execute hashMap getCmd 
        test <@ not (hashMap.ContainsKey(destKey)) && (RespUtils.nilBulkStr = actual) @>


    // may replace/augment the "... matches redis" tests with fscheck tests calling fredis and redis

    [<Fact>]
    static member ``"BITOP AND destKey srcKey1 srcKey2" vals different lengths, result matches redis`` () =
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
        let expected = sprintf "aba%s" xx |> StrToBulkStr
        test <@ expected =(FredisCmdProcessor.Execute hashMap getCmd ) @>


    [<Fact>]
    static member ``"BITOP OR destKey srcKey1 srcKey2" vals different lengths, result matches redis`` () =
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
        let expected = "qwgdef" |> StrToBulkStr // confirmed by trying this in redis
        test <@ expected = (FredisCmdProcessor.Execute hashMap getCmd ) @>



    [<Fact>]
    static member ``"BITOP XOR destKey srcKey1 srcKey2" vals different lengths, result matches redis`` () =
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
        let expected = [|16uy; 21uy; 6uy; 100uy; 101uy; 102uy|] |> RespUtils.MakeBulkStr
        test <@ expected = (FredisCmdProcessor.Execute hashMap getCmd ) @>



    [<Fact>]
    static member ``"BITOP NOT destKey srcKey" result matches redis`` () =
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
        let expected = RespUtils.MakeBulkStr [|0x9euy; 0x9duy; 0x9cuy; 0x9buy; 0x9auy; 0x99uy|] // confirmed by trying this in redis
        test <@ expected = (FredisCmdProcessor.Execute hashMap getCmd ) @>



    [<Fact>]
    static member ``"BITOP NOT destKey srcKey" srcKey does not exist, does not set destKey and returns (integer)0`` () =
        let hashMap = HashMap()
        let destKey = Key "destKey"
        let srcKey = Key "srcKey"
        let integerZero = Resp.Integer 0L
        let boi = FredisTypes.BitOpInner.NOT (destKey, srcKey)
        let bitopCmd = FredisCmd.BitOp boi
        let bitOpActual = FredisCmdProcessor.Execute hashMap bitopCmd 
        test <@ integerZero = bitOpActual && not (hashMap.ContainsKey destKey) @>




    [<Fact>]
    static member ``"BITOP AND destKey srcKey" destKey contains copy of srcKey val`` () =
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
        let expected = "val" |> StrToBulkStr
        test <@ expected  = (FredisCmdProcessor.Execute hashMap getCmd ) @>








type ``Parse GETRANGE`` () =
    let getRange    = "GETRANGE"  |> StrToBulkStr
    let key         = "key"       |> StrToBulkStr
    let kkey        = Key "key"
    let startIdx    = "0"         |> StrToBulkStr
    let endIdx      = "3"         |> StrToBulkStr

    [<Fact>]
    member this.``parse GETRANGE key start end returns FredisCmd.GetRange`` () = 
        let expected = FredisCmd.GetRange (kkey, 0, 3)
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [|getRange; key; startIdx; endIdx|] @>

    [<Fact>]
    member this.``parse GETRANGE fails when no params supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsGetRange = FredisCmdParser.ParseRESPtoFredisCmds [|getRange|] @>

    [<Fact>]
    member this.``parse GETRANGE key fails when no 'start end' params supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsGetRange = FredisCmdParser.ParseRESPtoFredisCmds [|getRange; key|] @>

    [<Fact>]
    member this.``parse GETRANGE key start fails when no 'end' param supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsGetRange = FredisCmdParser.ParseRESPtoFredisCmds [|getRange; key; startIdx|] @>







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

    [<Fact>]
    member this.``parse BITPOS fails when no params supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsBitpos = FredisCmdParser.ParseRESPtoFredisCmds [|bitPos|] @>

    [<Fact>]
    member this.``parse BITPOS key fails when no bit supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsBitpos = FredisCmdParser.ParseRESPtoFredisCmds [|bitPos; key|] @>

    [<Fact>]
    member this.``parse BITPOS key 9 fails as 9 is not convertable to a bit`` () = 
        test <@ Choice2Of2 ErrorMsgs.badBitArgBitpos = FredisCmdParser.ParseRESPtoFredisCmds [|bitPos; key; badBit|] @>

    [<Fact>]
    member this.``parse BITPOS key 0 succeeds`` () = 
        let expected = FredisCmd.Bitpos (kkey, false, ArrayRange.All)
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [|bitPos; key; bitArg0|] @>

    [<Fact>]
    member this.``parse BITPOS key 1 succeeds`` () = 
        let expected = FredisCmd.Bitpos (kkey, true, ArrayRange.All)
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [|bitPos; key; bitArg1|] @>

    [<Fact>]
    member this.``parse BITPOS key bitArg1 startByte succeeds`` () = 
        let nineByteOffset = (ByteOffset.create 9).Value
        let nineBlkStr   = "9" |> StrToBulkStr
        let expected = FredisCmd.Bitpos (kkey, true, ArrayRange.Lower nineByteOffset)
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [|bitPos; key; bitArg1; nineBlkStr|] @>

    [<Fact>]
    member this.``parse BITPOS key bitArg1 startByte endByte succeeds`` () = 
        let expected = FredisCmd.Bitpos (kkey, true, ``range (0,3)``)
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [|bitPos; key; bitArg1; startByte; endByte|] @>

    [<Fact>]
    member this.``parse BITPOS key true "not and int" fails with 'value not integer'`` () = 
        test <@ Choice2Of2 ErrorMsgs.valueNotIntegerOrOutOfRange = FredisCmdParser.ParseRESPtoFredisCmds [|bitPos; key; bitArg1; badStartByte|] @>

    [<Fact>]
    member this.``parse BITPOS key true 9 "not an int" fails with 'value not integer'`` () = 
        test <@ Choice2Of2 ErrorMsgs.valueNotIntegerOrOutOfRange = FredisCmdParser.ParseRESPtoFredisCmds [|bitPos; key; bitArg1; startByte; badEndByte|] @>




type ``Parse INCRBY`` () =

    let incrBy   = "INCRBY"   |> StrToBulkStr
    let skey = "key" |> StrToBulkStr
    let kkey = Key "key"
    let increment = "9" |> StrToBulkStr

    [<Fact>]
    member this.``parse INCRBY fails when no params supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsIncrBy = FredisCmdParser.ParseRESPtoFredisCmds [|incrBy|] @>

    [<Fact>]
    member this.``parse INCRBY fails when no increment supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsIncrBy = FredisCmdParser.ParseRESPtoFredisCmds [|incrBy; skey|] @>

    [<Fact>]
    member this.``parse INCRBY key incr succeeds`` () = 
        let expected = FredisCmd.IncrBy (kkey, 9L)
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [|incrBy; skey; increment |] @>






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




    [<Fact>]
    member this.``parse BITOP fails when no inner op supplied`` () = 
        test <@ Choice2Of2 ErrorMsgs.numArgsBitop = FredisCmdParser.ParseRESPtoFredisCmds [|bitop|] @>

//  the unquote version of this test, above, requires less type annotation and has better compile time type safety
//    [<Fact>]
//    member this.``parse BITOP fails when no inner op supplied fsunit`` () =
//        let expected:Choice<FredisCmd,byte []> = Choice2Of2 FredisErrorMsgs.numArgsBitop
//        expected |> should equal (BitopCmdProcessor.Parse [|bitop|])




    [<Fact>]
    member this.``parse "BITOP NOT destKey" fails with missing source key`` () =
        test <@ Choice2Of2 ErrorMsgs.numArgsBitop = FredisCmdParser.ParseRESPtoFredisCmds [| bitop; notOp; destKey |] @>



    [<Fact>]
    member this.``parse "BITOP NOT destKey srcKey1 srcKey1" fails with num keys error`` () =
        test <@ Choice2Of2 ErrorMsgs.numKeysBitopNot = FredisCmdParser.ParseRESPtoFredisCmds  [| bitop; notOp; destKey; srcKey1; srcKey2 |] @>



    [<Fact>]
    member this.``parse "BITOP OR destKey srcKey" succeeds`` () =
        let expected = FredisCmd.BitOp (BitOpInner.OR (kDestKey,[kSrcKey1]))
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [| bitop; orOp; destKey; srcKey1 |] @>



    [<Fact>]
    member this.``parse "BITOP NOT destKey srcKey" succeeds`` () =
        let expected = FredisCmd.BitOp (BitOpInner.NOT (kDestKey,kSrcKey1))
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [| bitop; notOp; destKey; srcKey1 |] @>



    [<Fact>]
    member this.``parse "BITOP AND destKey" fails with num keys error`` () =
        test <@ Choice2Of2 ErrorMsgs.numArgsBitop = FredisCmdParser.ParseRESPtoFredisCmds [| bitop; andOp; destKey |] @>



    [<Fact>]
    member this.``parse "BITOP AND destKey srcKey" succeeds`` () =
        let expected = FredisCmd.BitOp (BitOpInner.AND (kDestKey,[kSrcKey1]))
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [| bitop; andOp; destKey; srcKey1 |] @>



    [<Fact>]
    member this.``parse "BITOP AND destKey 4 src keys" succeeds`` () =
        let expected = FredisCmd.BitOp (BitOpInner.AND (kDestKey, [kSrcKey1; kSrcKey2; kSrcKey3; kSrcKey4]))
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [| bitop; andOp; destKey; srcKey1; srcKey2; srcKey3; srcKey4 |] @>


    [<Fact>]
    member this.``parse "BITOP XOR destKey srcKey" succeeds`` () =
        let expected = FredisCmd.BitOp (BitOpInner.XOR (kDestKey,[kSrcKey1]))
        test <@ Choice1Of2 expected = FredisCmdParser.ParseRESPtoFredisCmds [| bitop; xorOp; destKey; srcKey1 |] @>








