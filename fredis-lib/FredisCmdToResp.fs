module FredisCmdToResp

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


let keyBytesPairToBulkStrs (kk,bb) = [KeyToBulkStr kk; BytesToBulkStr bb]


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
    | AND (key, srcKey, srcKeys)   ->  [ yield StrToBulkStr "AND"; yield (KeyToBulkStr key);  yield (KeyToBulkStr srcKey);  yield! (keyListToBulkStrs srcKeys)  ]
    | OR  (key, srcKey, srcKeys)   ->  [ yield StrToBulkStr "OR";  yield (KeyToBulkStr key);  yield (KeyToBulkStr srcKey);  yield! (keyListToBulkStrs srcKeys)  ]
    | XOR (key, srcKey, srcKeys)   ->  [ yield StrToBulkStr "XOR"; yield (KeyToBulkStr key);  yield (KeyToBulkStr srcKey);  yield! (keyListToBulkStrs srcKeys)  ]
    | NOT (key1, key2)             ->  [ yield StrToBulkStr "NOT"; yield (KeyToBulkStr key1); yield (KeyToBulkStr key2)  ]


// F# does not have function overloading, but it does have member overloading
// helper class to allow the conversion to bulkstring based on the the type of the input param 
// all ToBS functions return a list of RESP bulk strings
[<AbstractClass;Sealed>]
type RespHlpr private () =
    static member ToBS (str:string)             = [ StrToBulkStr str  ]
    static member ToBS (key:Key)                = [ KeyToBulkStr key  ]
    static member ToBS (ii:int32)               = [ Int32ToBulkStr ii ]
    static member ToBS (ui:uint32)              = [ Int32ToBulkStr (int ui) ]
    static member ToBS (ii:int64)               = [ Int64ToBulkStr ii ]
    static member ToBS (bs:Bytes)               = [ BytesToBulkStr bs ]
    static member ToBS (ff:float)               = [ floatToBulkStr ff ]
    static member ToBS (bb:bool)                = [ boolToBulkStr bb  ]
    static member ToBS (kb:Key*Bytes)           = kb |> keyBytesPairToBulkStrs
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
        |GetBit         (key, uint)         -> [ RespHlpr.ToBS "GETBIT";         RespHlpr.ToBS key;        RespHlpr.ToBS uint                               ]
        |GetRange       (key, lower, upper) -> [ RespHlpr.ToBS "GETRANGE";       RespHlpr.ToBS key;        RespHlpr.ToBS lower;       RespHlpr.ToBS upper   ]
        |GetSet         (key, bs)           -> [ RespHlpr.ToBS "GETSET";         RespHlpr.ToBS key;        RespHlpr.ToBS bs                                 ]
        |Incr           key                 -> [ RespHlpr.ToBS "INCR";           RespHlpr.ToBS key                                                          ]
        |IncrBy         (key, amount)       -> [ RespHlpr.ToBS "INCRBY";         RespHlpr.ToBS key;        RespHlpr.ToBS amount                             ]
        |IncrByFloat    (key, amount)       -> [ RespHlpr.ToBS "INCRBYFLOAT";    RespHlpr.ToBS key;        RespHlpr.ToBS amount                             ]
        |MGet           (key, keys)         -> [ RespHlpr.ToBS "MGET";           RespHlpr.ToBS key;        RespHlpr.ToBS keys                               ]
        |MSet           (kv, kvs)           -> [ RespHlpr.ToBS "MSET";           RespHlpr.ToBS kv;         RespHlpr.ToBS kvs                                ]
        |MSetNX         (kv, kvs)           -> [ RespHlpr.ToBS "MSETNX";         RespHlpr.ToBS kv;         RespHlpr.ToBS kvs                                ]
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