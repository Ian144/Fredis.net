module FredisTypes






// http://redis.io/topics/protocol
// http://www.redisgreen.net/blog/reading-and-writing-redis-protocol/

// Errors:           "-"
// Integers:         ":"
// Bulk Strings:     "$"
// Simple String:    "+"
// Arrays:           "*"

// redis cant store 'strings' larger than 512mb, fredis copy this behaviour
let private maxByteOffset = (pown 2 29) - 1 // zero based, hence the -1
let private minByteOffset = (pown 2 29) * -1 // -1 represents the last element, so not zero based, and so there is no -1 

// constrained single case discriminated union
type ByteOffset = private ByteOffset of int with
    static member private isValid (ii:int) = (ii >= minByteOffset) && (ii <= maxByteOffset)
    
    static member create (ii:int) = 
        if ByteOffset.isValid ii
        then Some (ByteOffset ii) 
        else None

    static member createChoice (ii:int) (err: byte []) = 
        if ByteOffset.isValid ii
        then Choice1Of2 (ByteOffset ii) 
        else Choice2Of2 err

    // don't supply this property if it is a requirement to be opaque
    member this.Value with get () = 
                        let (ByteOffset ii) = this
                        ii



// bit offsets range of values is 32 bits (byte offset 2^29 * 2 ^ 3), so no constraint is required
//type BitOffset = private BitOffsetvalCtor of int with


type optByteOffsetPair = (ByteOffset*ByteOffset) option
type Bytes = byte array
type Key = Key of string


type BitOpInner = 
    |AND    of (Key * Key list)
    |OR     of (Key * Key list)
    |XOR    of (Key * Key list)
    |NOT    of (Key * Key)


type ArrayRange =
    | All
    | Lower of ByteOffset
    | LowerUpper of ByteOffset * ByteOffset



type FredisCmd = 
    |Ping
    |Get        of Key
    |Strlen     of Key
    |Set        of Key*Bytes
    |MSet       of (Key*Bytes) list
    |MGet       of Key list
    |Append     of Key*Bytes
    |Bitcount   of Key*optByteOffsetPair
    |BitOp      of BitOpInner
    |Decr       of Key
    |Incr       of Key
    |DecrBy     of Key*int64
    |IncrBy     of Key*int64
    |SetBit     of Key*int*bool
    |GetBit     of Key*int
    |GetSet     of Key*Bytes
    |Bitpos     of Key*bool*ArrayRange
    |GetRange   of Key*ArrayRange
//        |SetRange   of Key*int*Bytes


let BytesToStr bs = System.Text.Encoding.UTF8.GetString(bs)


// an empty bulk string e.g. "" is not the same as a bulk string that was not found
// without having this the Resp algebraic data type does not fully model RESP
type BulkStrContents = Nil | Contents of Bytes

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
//        | SimpleString bs   -> sprintf "SimpleString: %s" (BytesToStr bs)
//        | Error        _    -> "Error"
//        | Integer      ii   -> sprintf "Integer:%d" ii
//        | BulkString   cn   -> match cn with
//                                | BulkStrContents.Contents bs   -> sprintf "BulkString: %s" (BytesToStr bs)
//                                | BulkStrContents.Nil           -> "BulkString: nil"
//        | Array        _    -> "Array"

