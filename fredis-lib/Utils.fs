module Utils

//#### use less generic name than Utils and/or separate into multiple files

open FredisTypes



let BytesToStr bs = System.Text.Encoding.UTF8.GetString(bs)
let StrToBytes (str:string) = System.Text.Encoding.UTF8.GetBytes(str)   
let BytesToInt64 bs = System.BitConverter.ToInt64(bs, 0)
let BytesToKey = BytesToStr >> Key



let SetBit (bs:byte []) (index:int) (value:bool) =
    let byteIndex   = index / 8
    let bitIndex    = index % 8
    let mask = 1uy <<< bitIndex
    match value with
    | true  ->  bs.[byteIndex] <- bs.[byteIndex] ||| mask
                ()
    | false ->  bs.[byteIndex] <- bs.[byteIndex] &&& (~~~mask)
                ()


let GetBit (bs:byte []) (index:int) : bool =
    let byteIndex   = index / 8
    let bitIndex    = index % 8
    let mask = 1uy <<< bitIndex
    (bs.[byteIndex] &&& mask) <> 0uy



let OptionToChoice (optFunc:'a -> 'b option) (xx:'a) choice2Of2Val  = 
    match optFunc xx with
    | Some yy   -> Choice1Of2 yy
    | None      -> Choice2Of2 choice2Of2Val
                    
                    
let ChoiceParseInt failureMsg str :Choice<int,byte[]> = OptionToChoice FSharpx.FSharpOption.ParseInt str failureMsg


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


