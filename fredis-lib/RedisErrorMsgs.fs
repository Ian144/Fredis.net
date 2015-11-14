[<RequireQualifiedAccess>]
module ErrorMsgs



let numArgsSet                     = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'set' command")
let numArgsSetNX                   = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'setnx' command")
let numArgsGet                     = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'get' command")
let numArgsMSet                    = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'mset' command")
let numArgsMSetNX                  = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'msetnx' command")
let numArgsMGet                    = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'mget' command")
let numArgsAppend                  = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'append' command")
let numArgsBitcount                = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'bitcount' command")
let numArgsBitop                   = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'bitop' command")
let numArgsBitpos                  = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'bitpos' command")
let numArgsGetRange                = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'getrange' command")
let numArgsSetRange                = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'setrange' command")
let badBitArgBitpos                = System.Text.Encoding.UTF8.GetBytes("ERR The bit argument must be 1 or 0.");
let numKeysBitopNot                = System.Text.Encoding.UTF8.GetBytes("ERR BITOP NOT must be called with a single source key")

let numArgsDecr                    = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'decr' command")
let numArgsIncr                    = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'incr' command")
let numArgsDecrBy                  = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'decrby' command")
let numArgsIncrBy                  = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'incrby' command")
let numArgsIncrByFloat             = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'incrbyfloat' command")
let numArgsSetbit                  = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'setbit' command")
let numArgsGetbit                  = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'getbit' command")
let numArgsGetSet                  = System.Text.Encoding.UTF8.GetBytes("ERR wrong number of arguments for 'getset' command")


let syntaxError                     = System.Text.Encoding.UTF8.GetBytes("ERR syntax error")
let valueNotIntegerOrOutOfRange     = System.Text.Encoding.UTF8.GetBytes("ERR value is not an integer or out of range")
let valueNotAValidFloat             = System.Text.Encoding.UTF8.GetBytes("ERR value is not a valid float")
let bitOffsetNotIntegerOrOutOfRange = System.Text.Encoding.UTF8.GetBytes("ERR bit offset is not an integer or out of range")
let bitNotIntegerOrOutOfRange       = System.Text.Encoding.UTF8.GetBytes("ERR bit is not an integer or out of range")
