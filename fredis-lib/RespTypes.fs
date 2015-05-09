module RESPTypes






// http://redis.io/topics/protocol
// http://www.redisgreen.net/blog/reading-and-writing-redis-protocol/

// Errors:           "-"
// Integers:         ":"
// Bulk Strings:     "$"
// Simple String:    "+"
// Arrays:           "*"

type optIntPair = (int*int) option
type Bytes = byte array


type BitOpInner = 
        |AND    of (string* string list)
        |OR     of (string* string list)
        |XOR    of (string* string list)
        |NOT    of (string*string)


type ArrayRange =
    | All
    | Lower of int
    | LowerUpper of int * int


type FredisCmd = 
        |Ping
        |Get        of string
        |Strlen     of string
        |Set        of string*Bytes
        |MSet       of (string*Bytes) list
        |MGet       of string list
        |Append     of string*Bytes
        |Bitcount   of string*optIntPair
        |BitOp      of BitOpInner
        |Decr       of string
        |Incr       of string
        |DecrBy     of string*int64
        |IncrBy     of string*int64
        |SetBit     of string*int*bool
        |GetBit     of string*int
        |GetSet     of string*Bytes
        |Bitpos     of string*bool*ArrayRange
        |GetRange   of string*ArrayRange

type RESPMsg =
        | SimpleString   of Bytes
        | Error          of Bytes
        | Integer        of int64
        | BulkString     of Bytes
        | Array          of RESPMsg array

