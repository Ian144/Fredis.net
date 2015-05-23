module FredisTypes






// http://redis.io/topics/protocol
// http://www.redisgreen.net/blog/reading-and-writing-redis-protocol/

// Errors:           "-"
// Integers:         ":"
// Bulk Strings:     "$"
// Simple String:    "+"
// Arrays:           "*"

type optIntPair = (int*int) option
type Bytes = byte array
type Key = Key of string


type BitOpInner = 
        |AND    of (Key * Key list)
        |OR     of (Key * Key list)
        |XOR    of (Key * Key list)
        |NOT    of (Key * Key)


type ArrayRange =
    | All
    | Lower of int
    | LowerUpper of int * int



type FredisCmd = 
        |Ping
        |Get        of Key
        |Strlen     of Key
        |Set        of Key*Bytes
        |MSet       of (Key*Bytes) list
        |MGet       of Key list
        |Append     of Key*Bytes
        |Bitcount   of Key*optIntPair
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

type RESPMsg =
        | SimpleString   of Bytes
        | Error          of Bytes
        | Integer        of int64
        | BulkString     of Bytes
        | Array          of RESPMsg array

