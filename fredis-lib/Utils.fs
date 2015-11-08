module Utils

//#### use less generic name than Utils and/or separate into multiple files

open FSharpx.Choice


open FredisTypes



let BytesToStr bs = System.Text.Encoding.UTF8.GetString(bs)
let StrToBytes (str:string) = System.Text.Encoding.UTF8.GetBytes(str)
let BytesToInt64 bs = System.BitConverter.ToInt64(bs, 0)
let BytesToKey = BytesToStr >> Key


let keyToBytes = BytesToStr >> Key

let BitPosToRedis bitIndex (bs:byte array) =
    let maxBitIdx = bs.Length * 8 - 1           // match redis behaviour, the bit index starts at the opposite end
    let revBitIndex = maxBitIdx - bitIndex
    revBitIndex


let SetBit (bs:byte []) (bitIndexIn:int) (value:bool) =
    let bitIndex = BitPosToRedis bitIndexIn bs
    let byteIndex   = bitIndex / 8
    let bitIndex    = bitIndex % 8

    let mask = 1uy <<< bitIndex
    match value with
    | true  ->  bs.[byteIndex] <- bs.[byteIndex] ||| mask
                ()
    | false ->  bs.[byteIndex] <- bs.[byteIndex] &&& (~~~mask)
                ()


let GetBit (bs:byte []) (bitIndexIn:int) : bool =
    let bitIndex    = BitPosToRedis bitIndexIn bs    
    let byteIndex   = bitIndex / 8
    let bitIndex    = bitIndex % 8
    let mask = 1uy <<< bitIndex
    (bs.[byteIndex] &&& mask) <> 0uy



let OptionToChoice (optFunc:'a -> 'b option) (xx:'a) choice2Of2Val  = 
    match optFunc xx with
    | Some yy   -> Choice1Of2 yy
    | None      -> Choice2Of2 choice2Of2Val
                    
                    
let ChoiceParseInt failureMsg str : Choice<int,byte[]> = OptionToChoice FSharpx.FSharpOption.ParseInt str failureMsg



let ChoiceParsePosOrZeroInt failureMsg str : Choice<int,byte[]> = 
    choose{
        let! ii1 = ChoiceParseInt failureMsg str
        let! ii2 = if ii1 < 0 then Choice2Of2 failureMsg else Choice1Of2 ii1
        return ii2
    }

let ChoiceParseBoolFromInt (errorMsg:byte[]) (ii:int) = 
    match ii with
    | 1 -> Choice1Of2 true
    | 0 -> Choice1Of2 false
    | _ -> Choice2Of2 errorMsg


let ChoiceParseBool (errorMsg) (ss) = 
    match ss with
    | "1"   -> Choice1Of2 true
    | "0"   -> Choice1Of2 false
    | _     -> Choice2Of2 errorMsg


