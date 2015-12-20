module FredisTypes






// http://redis.io/topics/protocol
// http://www.redisgreen.net/blog/reading-and-writing-redis-protocol/

// Errors:           "-"
// Integers:         ":"
// Bulk Strings:     "$"
// Simple String:    "+"
// Arrays:           "*"

// redis cant store 'strings' larger than 512mb
let private maxByteOffset = (pown 2 29) - 1 // zero based, hence the -1
let private minByteOffset = (pown 2 29) * -1 

// constrained single case discriminated union
[<StructuredFormatDisplay("{FormatDisplay}")>]
type ByteOffset = private ByteOffset of int with
    static member private IsValid (ii:int) = (ii >= minByteOffset) && (ii <= maxByteOffset)
    
    static member Create (ii:int) = 
        if ByteOffset.IsValid ii
        then Some (ByteOffset ii) 
        else None

    static member CreateChoice (ii:int) (err: byte []) = 
        if ByteOffset.IsValid ii
        then Choice1Of2 (ByteOffset ii) 
        else Choice2Of2 err

    // don't supply this property if it is a requirement to be opaque
    member public this.Value with get () = 
                                    let (ByteOffset ii) = this
                                    ii

    member this.FormatDisplay = match this with ByteOffset ii -> sprintf "ByteOffset: %d" ii



// bit offsets range of values is 32 bits (byte offset 2^29 * 2 ^ 3), so no constraint is required
//type BitOffset = private BitOffsetvalCtor of int with

type optByteOffsetPair = (ByteOffset*ByteOffset) option
type Bytes = byte array 
type Key = Key of string

// AND, OR and XOR need a destKey, srcKey and 0->N further source keys
// making 'illegal states unrepresentable' by not having all source keys in a list, as that would allow zero source keys
type BitOpInner = 
    |AND    of (Key * Key * Key list)   // must be one destKey, one scrKey, and an optional number of further source keys
    |OR     of (Key * Key * Key list)
    |XOR    of (Key * Key * Key list)
    |NOT    of (Key * Key)


type ArrayRange =
    | All
    | Lower of ByteOffset
    | LowerUpper of ByteOffset * ByteOffset


// to simplify 
type KeyBytes = (Key*Bytes)


// making illegal states unrepresentable
// 1. MGET must have at least one key, with an optional number of further keys, hence "|MGet of Key * Key list" and not "|MGet of Key list", so the type system enforces this
// 2. MSET and MSETNX, as above but for KeyBytesPair
// 3. SetBit and GetBit cannot have negative indices, hence unsigned int
// these changes help fscheck to automatically generate FredisCmds, however ByteOffsets require custom generators for their 29 bit range

// 
let BytesToStr  = System.Text.Encoding.UTF8.GetString

[<StructuredFormatDisplay("{FormatDisplay}")>]
type FredisCmd = 
    |Append         of Key*Bytes
    |Bitcount       of Key*optByteOffsetPair
    |BitOp          of BitOpInner
    |Bitpos         of Key*bool*ArrayRange
    |Decr           of Key
    |DecrBy         of Key*int64
    |FlushDB
    |Get            of Key
    |GetBit         of Key*uint32
    |GetRange       of Key*int*int
    |GetSet         of Key*Bytes
    |Incr           of Key 
    |IncrBy         of Key*int64
    |IncrByFloat    of Key*double
    |MGet           of Key*Key list
    |MSet           of KeyBytes*KeyBytes list
    |MSetNX         of KeyBytes*KeyBytes list
    |Ping
    |Set            of Key*Bytes
    |SetBit         of Key*uint32*bool
    |SetNX          of Key*Bytes
    |SetRange       of Key*uint32*Bytes
    |Strlen         of Key
    member this.FormatDisplay =
        match this with
        |Append         (key, bs)               -> sprintf "Append %A %s" key (BytesToStr bs)
        |Bitcount       (key, optOffsetPair)    -> sprintf "Bitcount %A %A" key optOffsetPair
        |BitOp          (bitOpInner)            -> sprintf "BitOp %A" bitOpInner
        |Bitpos         (key, bb, range)        -> sprintf "Bitpos %A %A %A" key bb range
        |Decr           (key)                   -> sprintf "Decr %A" key
        |DecrBy         (key, ii)               -> sprintf "DecrBy %A %d" key ii
        |Get            (key)                   -> sprintf "Get %A" key 
        |GetBit         (key, uii)              -> sprintf "GetBit %A %d" key uii
        |GetRange       (key, lower, upper)     -> sprintf "GetRange %A %d %d" key lower upper
        |GetSet         (key, bs)               -> sprintf "GetSet %A %A" key (BytesToStr bs)
        |Incr           (key)                   -> sprintf "Incr %A" key
        |IncrBy         (key, ii)               -> sprintf "IncrBy %A %d" key ii
        |IncrByFloat    (key, ff)               -> sprintf "IncrByFloat %A %f" key ff
        |MGet           (key, keys)             -> sprintf "MGet %A %A" key keys
        |MSet           (keyBs, keyBss)         -> sprintf "MSet %A %A" keyBs keyBss
        |MSetNX         (keyBs, keyBss)         -> sprintf "MSetNX %A %A" keyBs keyBss
        |Set            (key, bs)               -> sprintf "Set %A %A" key (BytesToStr bs)
        |SetBit         (key, uii, bb)          -> sprintf "SetBit %A %d %A" key uii bb
        |SetNX          (key, bs)               -> sprintf "SetNX %A %A" key bs
        |SetRange       (key, uii, bs)          -> sprintf "SetRange %A %d %A" key uii (BytesToStr bs)
        |Strlen         (key)                   -> sprintf "Strlen %A" key
        |FlushDB                                -> sprintf "FlushDB"
        |Ping                                   -> sprintf "Ping"



// an empty bulk string e.g. "" is not the same as a bulk string that was not found
// without having this the Resp algebraic data type does not fully model RESP

//type BulkStrContents = Nil | Contents of Bytes

[<StructuredFormatDisplay("{FormatDisplay}")>]
type BulkStrContents = 
    | Nil 
    | Contents of Bytes
    member this.FormatDisplay =
        match this with
        | Nil           -> "Nil"
        | Contents bs   -> (BytesToStr bs) |> sprintf "BulkStr: %s" 





type Resp =
    | SimpleString   of Bytes
    | Error          of Bytes
    | Integer        of int64
    | BulkString     of BulkStrContents
    | Array          of Resp array


//[<StructuredFormatDisplay("{FormatDisplay}")>]
//type Resp =
//    | SimpleString   of Bytes
//    | Error          of Bytes
//    | Integer        of int64
//    | BulkString     of BulkStrContents
//    | Array          of Resp array
//
//    member this.FormatDisplay =
//        match this with
//        | SimpleString bs   ->  sprintf "SimpleString: %s" (BytesToStr bs)
//        | Error        bs   ->  sprintf "Error: %s" (BytesToStr bs)
//        | Integer      ii   ->  sprintf "Integer:%d" ii
//        | BulkString   cn   ->  match cn with
//                                | BulkStrContents.Contents bs   -> sprintf "%s" (BytesToStr bs)
//                                | BulkStrContents.Nil           -> "nil"
//        | Array        xs   ->  let subStrs = xs |> Array.map (fun x -> x.FormatDisplay )
//                                let subStr = System.String.Join(", ", subStrs)
//                                System.String.Format("[|{0}|]", subStr)
                                

