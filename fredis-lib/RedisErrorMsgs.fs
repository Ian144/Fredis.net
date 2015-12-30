[<RequireQualifiedAccess>]
module ErrorMsgs



let badBitArgBitpos                 = "ERR The bit argument must be 1 or 0."B
let bitNotIntegerOrOutOfRange       = "ERR bit is not an integer or out of range"B
let bitOffsetNotIntegerOrOutOfRange = "ERR bit offset is not an integer or out of range"B
let numArgsAppend                   = "ERR wrong number of arguments for 'append' command"B
let numArgsBitcount                 = "ERR wrong number of arguments for 'bitcount' command"B
let numArgsBitop                    = "ERR wrong number of arguments for 'bitop' command"B
let numArgsBitpos                   = "ERR wrong number of arguments for 'bitpos' command"B
let numArgsDecr                     = "ERR wrong number of arguments for 'decr' command"B
let numArgsDecrBy                   = "ERR wrong number of arguments for 'decrby' command"B
let numArgsGet                      = "ERR wrong number of arguments for 'get' command"B
let numArgsGetbit                   = "ERR wrong number of arguments for 'getbit' command"B
let numArgsGetRange                 = "ERR wrong number of arguments for 'getrange' command"B
let numArgsGetSet                   = "ERR wrong number of arguments for 'getset' command"B
let numArgsIncr                     = "ERR wrong number of arguments for 'incr' command"B
let numArgsIncrBy                   = "ERR wrong number of arguments for 'incrby' command"B
let numArgsIncrByFloat              = "ERR wrong number of arguments for 'incrbyfloat' command"B
let numArgsMGet                     = "ERR wrong number of arguments for 'mget' command"B
let numArgsMSet                     = "ERR wrong number of arguments for 'mset' command"B
let numArgsMSetNX                   = "ERR wrong number of arguments for 'msetnx' command"B
let numArgsSet                      = "ERR wrong number of arguments for 'set' command"B
let numArgsSetbit                   = "ERR wrong number of arguments for 'setbit' command"B
let numArgsSetNX                    = "ERR wrong number of arguments for 'setnx' command"B
let numArgsSetRange                 = "ERR wrong number of arguments for 'setrange' command"B
let numKeysBitopNot                 = "ERR BITOP NOT must be called with a single source key"B
let syntaxError                     = "ERR syntax error"B
let valueNotAValidFloat             = "ERR value is not a valid float"B
let valueNotIntegerOrOutOfRange     = "ERR value is not an integer or out of range"B

