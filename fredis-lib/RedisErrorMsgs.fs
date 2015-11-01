[<RequireQualifiedAccess>]
module ErrorMsgs


//#### remove the duplication
//#### use a single case DU / phantom type for this, other things in fredis.net are also byte arrays



let numArgsSet                     = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'set' command\r\n")
let numArgsSetNX                   = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'setnx' command\r\n")
let numArgsGet                     = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'get' command\r\n")
let numArgsMSet                    = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'mset' command\r\n")
let numArgsMSetNX                  = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'msetnx' command\r\n")
let numArgsMGet                    = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'mget' command\r\n")
let numArgsAppend                  = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'append' command\r\n")
let numArgsBitcount                = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'bitcount' command\r\n")
let numArgsBitop                   = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'bitop' command\r\n")
let numArgsBitpos                  = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'bitpos' command\r\n")
let numArgsGetRange                = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'getrange' command\r\n")
let numArgsSetRange                = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'setrange' command\r\n")
let badBitArgBitpos                = System.Text.Encoding.UTF8.GetBytes("-ERR The bit argument must be 1 or 0\r\n.");
let numKeysBitopNot                = System.Text.Encoding.UTF8.GetBytes("-ERR BITOP NOT must be called with a single source key\r\n")

let numArgsDecr                    = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'decr' command\r\n")
let numArgsIncr                    = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'incr' command\r\n")
let numArgsDecrBy                  = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'decrby' command\r\n")
let numArgsIncrBy                  = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'incrby' command\r\n")
let numArgsIncrByFloat             = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'incrbyfloat' command\r\n")
let numArgsSetbit                  = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'setbit' command\r\n")
let numArgsGetbit                  = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'getbit' command\r\n")
let numArgsGetSet                  = System.Text.Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'getset' command\r\n")


let syntaxError                     = System.Text.Encoding.UTF8.GetBytes("-ERR syntax error\r\n")
let valueNotIntegerOrOutOfRange     = System.Text.Encoding.UTF8.GetBytes("-ERR value is not an integer or out of range\r\n")
let valueNotAValidFloat             = System.Text.Encoding.UTF8.GetBytes("-ERR value is not a valid float\r\n")
let bitOffsetNotIntegerOrOutOfRange = System.Text.Encoding.UTF8.GetBytes("-ERR bit offset is not an integer or out of range\r\n")
let bitNotIntegerOrOutOfRange       = System.Text.Encoding.UTF8.GetBytes("-ERR bit is not an integer or out of range\r\n")
