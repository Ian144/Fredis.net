module Fredis_Fedis_Test

open FsCheck
open FsCheck.Xunit

open FredisTypes




let BytesToBulkStr = BulkStrContents.Contents >> Resp.BulkString
let StrToBulkStr = Utils.StrToBytes >> BytesToBulkStr
let KeyToStr (kk:Key) =
    let (Key str) = kk
    str
let KeyToBulkStr = KeyToStr >> StrToBulkStr

let Int32ToBulkStr (ii:int32) = sprintf "%d" ii |> StrToBulkStr
let Int64ToBulkStr (ii:int64) = sprintf "%d" ii |> StrToBulkStr
//let floatToBulkStr (ff:float) = sprintf "%f" ff |> StrToBulkStr
let floatToBulkStr (ff:float) = System.Convert.ToString ff |> StrToBulkStr
let boolToBulkStr  (bb:bool)  = 
    match bb with
    | true  -> "1" |> StrToBulkStr
    | false -> "0" |> StrToBulkStr

let keyListToBulkStrs (ks:Key list) = ks |> List.map KeyToBulkStr

let keyBytesListToBulkStrs (kbs:(Key*Bytes) list) = 
    [   for kk,bs in kbs do
        yield kk |> KeyToBulkStr
        yield bs |> BytesToBulkStr  ]

let ByteOffsetToBulkStr (byteOffset:FredisTypes.ByteOffset) = byteOffset.Value |> Int32ToBulkStr
let ByteOffsetPairToBulkStrs (bo1,bo2) = [ByteOffsetToBulkStr bo1; ByteOffsetToBulkStr bo2]
let OptByteOffsetPairToBulkStrs optBo = 
    match optBo with
    |Some bo    -> ByteOffsetPairToBulkStrs bo
    |None       -> []



let ArrayRangeToBulkStrs (rng:ArrayRange) = 
    match rng with
    | All                   -> []
    | Lower bo              -> [ByteOffsetToBulkStr bo]
    | LowerUpper (ll, uu)   -> [ByteOffsetToBulkStr ll; ByteOffsetToBulkStr uu]
    



// RespHlpr is not available until further down this source file
let BitOpInnerToBulkStrs (boi:FredisTypes.BitOpInner) = 
    match boi with
    | AND (key, keys)   ->  [ yield StrToBulkStr "AND"; yield (KeyToBulkStr key);  yield! (keyListToBulkStrs keys)  ]
    | OR  (key, keys)   ->  [ yield StrToBulkStr "OR";  yield (KeyToBulkStr key);  yield! (keyListToBulkStrs keys)  ]
    | XOR (key, keys)   ->  [ yield StrToBulkStr "XOR"; yield (KeyToBulkStr key);  yield! (keyListToBulkStrs keys)  ]
    | NOT (key1, key2)  ->  [ yield StrToBulkStr "NOT"; yield (KeyToBulkStr key1); yield  (KeyToBulkStr      key2)  ]


// F# does not have function overloading, but it does have member overloading
// helper class to allow the conversion to bulkstring based on the the type of the input param 
[<AbstractClass;Sealed>]
type RespHlpr private () =
    static member ToBS (str:string)             = [ StrToBulkStr str  ]
    static member ToBS (key:Key)                = [ KeyToBulkStr key  ]
    static member ToBS (ii:int32)               = [ Int32ToBulkStr ii ]
    static member ToBS (ii:int64)               = [ Int64ToBulkStr ii ]
    static member ToBS (bs:Bytes)               = [ BytesToBulkStr bs ]
    static member ToBS (ff:float)               = [ floatToBulkStr ff ]
    static member ToBS (bb:bool)                = [ boolToBulkStr bb  ]
    static member ToBS (xx:optByteOffsetPair)   = xx |> OptByteOffsetPairToBulkStrs
    static member ToBS (xx:BitOpInner)          = xx |> BitOpInnerToBulkStrs
    static member ToBS (xx:ArrayRange)          = xx |> ArrayRangeToBulkStrs
    static member ToBS (xs:Key list)            = xs |> keyListToBulkStrs
    static member ToBS (xs:(Key*Bytes) list)    = xs |> keyBytesListToBulkStrs



let FredisCmdToRESP (cmd:FredisTypes.FredisCmd) =
    let xss = 
        match cmd with
        |Append         (key, bytes)        -> [ RespHlpr.ToBS "APPEND";         RespHlpr.ToBS key;        RespHlpr.ToBS bytes                              ]
        |Bitcount       (key, optOffsets)   -> [ RespHlpr.ToBS "BITCOUNT";       RespHlpr.ToBS key;        RespHlpr.ToBS optOffsets                         ]
        |BitOp          bitOpInner          -> [ RespHlpr.ToBS "BITOP";          RespHlpr.ToBS bitOpInner                                                   ]
        |Bitpos         (key, bb, range)    -> [ RespHlpr.ToBS "BITPOS";         RespHlpr.ToBS key;        RespHlpr.ToBS bb;          RespHlpr.ToBS range   ]
        |Decr           key                 -> [ RespHlpr.ToBS "DECR";           RespHlpr.ToBS key                                                          ]
        |DecrBy         (key, amount)       -> [ RespHlpr.ToBS "DECRBY";         RespHlpr.ToBS key;        RespHlpr.ToBS amount                             ]
        |Get            key                 -> [ RespHlpr.ToBS "GET";            RespHlpr.ToBS key                                                          ]
        |GetBit         (key, int)          -> [ RespHlpr.ToBS "GETBIT";         RespHlpr.ToBS key;        RespHlpr.ToBS int                                ]
        |GetRange       (key, lower, upper) -> [ RespHlpr.ToBS "GETRANGE";       RespHlpr.ToBS key;        RespHlpr.ToBS lower;       RespHlpr.ToBS upper   ]
        |GetSet         (key, bs)           -> [ RespHlpr.ToBS "GETSET";         RespHlpr.ToBS key;        RespHlpr.ToBS bs                                 ]
        |Incr           key                 -> [ RespHlpr.ToBS "INCR";           RespHlpr.ToBS key                                                          ]
        |IncrBy         (key, amount)       -> [ RespHlpr.ToBS "INCRBY";         RespHlpr.ToBS key;        RespHlpr.ToBS amount                             ]
        |IncrByFloat    (key, amount)       -> [ RespHlpr.ToBS "INCRBYFLOAT";    RespHlpr.ToBS key;        RespHlpr.ToBS amount                             ]
        |MGet           keys                -> [ RespHlpr.ToBS "MGET";           RespHlpr.ToBS keys                                                         ]
        |MSet           keyVals             -> [ RespHlpr.ToBS "MSET";           RespHlpr.ToBS keyVals                                                      ]
        |MSetNX         keyVals             -> [ RespHlpr.ToBS "MSETNX";         RespHlpr.ToBS keyVals                                                      ]
        |Set            (key, bs)           -> [ RespHlpr.ToBS "SET";            RespHlpr.ToBS key;        RespHlpr.ToBS bs                                 ]
        |SetBit         (key, pos, vval)    -> [ RespHlpr.ToBS "SETBIT";         RespHlpr.ToBS key;        RespHlpr.ToBS pos;         RespHlpr.ToBS vval    ]
        |SetNX          (key, bs)           -> [ RespHlpr.ToBS "SETNX";          RespHlpr.ToBS key;        RespHlpr.ToBS bs                                 ]
        |SetRange       (key, pos, bytes)   -> [ RespHlpr.ToBS "SETRANGE";       RespHlpr.ToBS key;        RespHlpr.ToBS pos;         RespHlpr.ToBS bytes   ]
        |Strlen         key                 -> [ RespHlpr.ToBS "STRLEN";         RespHlpr.ToBS key                                                          ]
        |Ping                               -> [ RespHlpr.ToBS "PING"                                                                                       ]
        |FlushDB                            -> [ RespHlpr.ToBS "FLUSHDB"                                                                                    ]
    
    // flatten the list of lists and convert the result to an array, RESP is read from TCP into arrays
    let xs = 
        [   for xs in xss do
            yield! xs ]
    xs |> List.toArray





let fredisCmdEquality cmd1 cmd2 = 

    // 'wacky because it considers NaN to be equal to NaN
    // definition nested inside fredisCmdEquality to hide from other potential users
    let WackyEquality ff1 ff2 = 
        let ff1IsNan = System.Double.IsNaN ff1
        let ff2IsNan = System.Double.IsNaN ff2
        match ff1IsNan, ff2IsNan with
        | true, true    -> true
        | _, _          -> System.Math.Abs (ff1 - ff2) < 0.0000001

    match cmd1, cmd2 with
    | IncrByFloat (key1, amount1), IncrByFloat (key2, amount2) ->   let keysEq = key1 = key2
                                                                    let amountsEq = WackyEquality amount1 amount2
                                                                    keysEq && amountsEq
    | _, _                                                     ->   cmd1 = cmd2



(*
    http://blog.ploeh.dk/2015/01/10/diamond-kata-with-fscheck/

    type Letters =
        static member Char() =
            Arb.Default.Char()
            |> Arb.filter (fun c -> 'A' <= c && c <= 'Z')
 
    type DiamondPropertyAttribute() =
        inherit PropertyAttribute(
            Arbitrary = [| typeof<Letters> |],
            QuietOnSuccess = true)
 
    [<DiamondProperty>]
    let ``Diamond is non-empty`` (letter : char) =
        let actual = Diamond.make letter
        not (String.IsNullOrWhiteSpace actual)


    Ad hoc Arbitraries with FsCheck.Xunit

    [<Property>]
    let ``Any live cell with more than three live neighbors dies``
        (cell : int * int)
        (neighborCount : int) =
        (3 < neighborCount && neighborCount <= 8) ==> lazy
 
        let neighborCells = findNeighbors cell |> pickRandom neighborCount
        let actual = calculateNextState (cell :: neighborCells) cell
        Dead =! actual


    http://stackoverflow.com/questions/25026976/preventing-fscheck-from-generating-nan-and-infinities
    (in nested, reflexivly generated types)

*)


// how much do problems relate to 
//  representable invalid states - e.g. potentially null strings
//  yield! removing empty lists
//  fp equality rounding - write a FredisCmd equality func




let key1 = Gen.constant (Key "key1")
let key2 = Gen.constant (Key "key2")
let key3 = Gen.constant (Key "key3")
let key4 = Gen.constant (Key "key4")
let key5 = Gen.constant (Key "key5")
let key6 = Gen.constant (Key "key6")
let key7 = Gen.constant (Key "key7")
let key8 = Gen.constant (Key "key8")
let genKey = Gen.frequency[(1, key1); (1, key2); (1, key3); (1, key4); (1, key5); (1, key6); (1, key7); (1, key8) ]


// create an Arbitrary<ByteOffset> so as to avoid the runtime error below
// "The type FredisTypes+ByteOffset is not handled automatically by FsCheck. Consider using another type or writing and registering a generator for it"

let private maxByteOffset = (pown 2 29) - 1 // zero based, hence the -1
let private minByteOffset = (pown 2 29) * -1 


let genByteOffset = 
    Gen.choose(minByteOffset, maxByteOffset)
    |> Gen.map FredisTypes.ByteOffset.create
    |> Gen.map (fun optBoffset -> optBoffset.Value)





// overrides created to apply to nest values in reflexively generated types

type Overrides() =
    static member Float() =
        Arb.Default.Float()
        |> Arb.filter (fun f -> not <| System.Double.IsNaN(f) && 
                                not <| System.Double.IsInfinity(f) &&
                                not <| (System.Math.Abs(f) > (System.Double.MaxValue / 2.0)) )

    // creates stack overflow, probably because NormalFloat calls the overridden Float
//    static member Float() =
//        Arb.generate<NormalFloat>
//        |> Gen.map (fun (NormalFloat ff) -> ff)
//        |> Arb.fromGen

    static member NonEmptyKeyList() = Gen.listOf genKey |> Arb.fromGen |> Arb.filter (fun xs -> List.isEmpty xs |> not)
    static member Key() = Arb.fromGen genKey
    static member ByteOffsets() = Arb.fromGen genByteOffset




[<Property( Arbitrary=[|typeof<Overrides>|], Verbose=true, MaxTest=1000 )>]
let ``fredis cmd to resp to fredis cmd roundtrip`` (cmdIn:FredisTypes.FredisCmd) =
    let resp = FredisCmdToRESP cmdIn
    match  FredisCmdParser.ParseRESPtoFredisCmds resp with
    | Choice1Of2 cmdOut   -> fredisCmdEquality cmdIn cmdOut
    | Choice2Of2 _        -> false






[<Property( Arbitrary=[|typeof<Overrides>|], Verbose=true, MaxTest=100 )>]
let ``test funcx`` (cmds:FredisTypes.FredisCmd list) =


    printfn "%A" cmds
    true