module Fredis_Fedis_Test

open FsCheck
open FsCheck.Xunit

open FredisTypes




let fredisCmdEquality cmd1 cmd2 = 

    let approxEq (ff1:float) (ff2:float) = 
        let diff = System.Math.Abs (ff1 - ff2)
        diff < 0.0000001

    match cmd1, cmd2 with
    | IncrByFloat (key1, amount1), IncrByFloat (key2, amount2) ->   let keysEq = key1 = key2
                                                                    let amountsEq = approxEq amount1 amount2
                                                                    keysEq && amountsEq
    | _, _                                                     ->   cmd1 = cmd2



(*
    http://blog.ploeh.dk/2015/01/10/diamond-kata-with-fscheck/

    type Letters =
        static member Char() =
            Arb.Default.Char()
            |> Arb.filter (fun c -> 'A' <= c && c <= 'Z')
 
    type DiamondPropertyAttribute() =
        inherit PropertyAttribute(
            Arbitrary = [| typeof<Letters> |],
            QuietOnSuccess = true)
 
    [<DiamondProperty>]
    let ``Diamond is non-empty`` (letter : char) =
        let actual = Diamond.make letter
        not (String.IsNullOrWhiteSpace actual)


    Ad hoc Arbitraries with FsCheck.Xunit

    [<Property>]
    let ``Any live cell with more than three live neighbors dies``
        (cell : int * int)
        (neighborCount : int) =
        (3 < neighborCount && neighborCount <= 8) ==> lazy
 
        let neighborCells = findNeighbors cell |> pickRandom neighborCount
        let actual = calculateNextState (cell :: neighborCells) cell
        Dead =! actual


    http://stackoverflow.com/questions/25026976/preventing-fscheck-from-generating-nan-and-infinities
    (in nested, reflexivly generated types)

*)



let key1 = Gen.constant (Key "key1")
let key2 = Gen.constant (Key "key2")
let key3 = Gen.constant (Key "key3")
let key4 = Gen.constant (Key "key4")
let key5 = Gen.constant (Key "key5")
let key6 = Gen.constant (Key "key6")
let key7 = Gen.constant (Key "key7")
let key8 = Gen.constant (Key "key8")
let genKey = Gen.frequency[(1, key1); (1, key2); (1, key3); (1, key4); (1, key5); (1, key6); (1, key7); (1, key8) ]


// create an Arbitrary<ByteOffset> so as to avoid the runtime error below
// "The type FredisTypes+ByteOffset is not handled automatically by FsCheck. Consider using another type or writing and registering a generator for it"

let private maxByteOffset = (pown 2 29) - 1 // zero based, hence the -1
let private minByteOffset = (pown 2 29) * -1 


let genByteOffset = 
    Gen.choose(minByteOffset, maxByteOffset)
    |> Gen.map FredisTypes.ByteOffset.Create
    |> Gen.map (fun optBoffset -> optBoffset.Value)



let genByte = Gen.arrayOf Arb.generate<byte>
    
let genKeyBytePair =
    gen{
        let! key = genKey
        let! bytes = genByte
        return key, bytes
    }
    


// overrides created apply to nested values in reflexively generated types (FredisCmds)
type Overrides() =
    static member Float() =
        Arb.Default.Float()
        |> Arb.filter (fun f -> not <| System.Double.IsNaN(f) && 
                                not <| System.Double.IsInfinity(f) &&
                                (System.Math.Abs(f) < (System.Double.MaxValue / 2.0)) )

    static member Key() = Arb.fromGen genKey
    static member ByteOffsets() = Arb.fromGen genByteOffset




[<Property( Arbitrary=[|typeof<Overrides>|], Verbose=true, MaxTest=1000 )>]
let ``fredis cmd to resp to fredis cmd roundtrip`` (cmdIn:FredisTypes.FredisCmd) =
    let resp = FredisCmdToResp.FredisCmdToRESP cmdIn
    match  FredisCmdParser.ParseRESPtoFredisCmds resp with
    | Choice1Of2 cmdOut   ->    fredisCmdEquality cmdIn cmdOut
    | Choice2Of2 bs       ->    let msg = Utils.BytesToStr bs
                                System.Diagnostics.Debug.WriteLine msg 
                                false

