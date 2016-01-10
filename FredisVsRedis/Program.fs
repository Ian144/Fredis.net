
open System.Net.Sockets
open FsCheck
open FredisTypes
open RespStreamFuncs




let fredisCmdEquality cmd1 cmd2 = 

    let approxEq (ff1:float) (ff2:float) = 
        let diff = System.Math.Abs (ff1 - ff2)
        diff < 0.0000001

    match cmd1, cmd2 with
    | IncrByFloat (key1, amount1), IncrByFloat (key2, amount2) ->   let keysEq = key1 = key2
                                                                    let amountsEq = approxEq amount1 amount2
                                                                    keysEq && amountsEq
    | _, _                                                     ->   cmd1 = cmd2



let key1 = Gen.constant (Key "key1")
let key2 = Gen.constant (Key "key2")
let key3 = Gen.constant (Key "key3")
let key4 = Gen.constant (Key "key4")
let key5 = Gen.constant (Key "key5")
let key6 = Gen.constant (Key "key6")
let key7 = Gen.constant (Key "key7")
let key8 = Gen.constant (Key "key8")
//let genKey = Gen.frequency[(1, key1); (1, key2); (1, key3); (1, key4); (1, key5); (1, key6); (1, key7); (1, key8) ]
let genKey = Gen.frequency[(1, key1); (1, key2); (1, key3); (1, key4) ]


// create an Arbitrary<ByteOffset> so as to avoid the runtime error below
// "The type FredisTypes+ByteOffset is not handled automatically by FsCheck. Consider using another type or writing and registering a generator for it"

let private maxByteOffset = (pown 2 29) - 1 // zero based, hence the -1
let private minByteOffset = (pown 2 29) * -1 


let genByteOffset = 
    Gen.choose(minByteOffset, maxByteOffset)
    |> Gen.map FredisTypes.ByteOffset.Create
    |> Gen.map (fun optBoffset -> optBoffset.Value)

let shrinkByteOffset (bo:ByteOffset) = 
    match bo.Value with
    | 0 ->  Seq.empty
    | n ->  Arb.shrink n
            |> Seq.map (ByteOffset.Create >> (fun opt -> opt.Value))
     
    
let genAlphaByte = Gen.choose(65,90) |> Gen.map byte 
let genAlphaByteArray = Gen.arrayOfLength 16 genAlphaByte 



let FredisCmdFilterNoNumOps cmd = 
    match cmd with
    | IncrByFloat _ | IncrBy _  | Incr _    -> false
    | Decr _  | DecrBy _                    -> false
    | _                                     -> true

 

let FredisCmdFilterBitOps cmd = 
    match cmd with
    | SetBit _ | BitOp _  | Bitpos _  | Set _   -> true
    | _                                         -> false



let genFredisCmd = Arb.generate<FredisCmd>



let shrinkFredisCmd (cmd:FredisCmd) = 
    match cmd with
    |Append         (key, bs)               ->  Arb.shrink bs |> Seq.map (fun bs2 -> Append (key, bs2) )
    |Bitcount       (key, optOffsetPair)    ->  Arb.shrink optOffsetPair |> Seq.map (fun optBo2 -> Bitcount (key, optBo2))
    |BitOp          (bitOpInner)            ->  Arb.shrink bitOpInner |> Seq.map BitOp 
    |Bitpos         (key, bb, range)        ->  Arb.shrink range |> Seq.map (fun r2 -> Bitpos (key, bb, r2))
    |Decr           _                       ->  Seq.empty
    |DecrBy         (key, ii)               ->  Arb.shrink ii |> Seq.map (fun bs2 -> DecrBy (key, bs2) )
    |Get            _                       ->  Seq.empty
    |GetBit         (key, uii)              ->  Arb.shrink uii |> Seq.map (fun uii2-> GetBit (key, uii2) )
    |GetRange       (key, lower, upper)     ->  Arb.shrink (lower, upper) |> Seq.map (fun t2-> GetRange (key, (fst t2), (snd t2) ) )
    |GetSet         (key, bs)               ->  Arb.shrink bs |> Seq.map (fun bs2 -> GetSet (key, bs2) )
    |Incr           _                       ->  Seq.empty
    |IncrBy         (key, ii)               ->  Arb.shrink ii |> Seq.map (fun ii2 -> IncrBy (key, ii2) )
    |IncrByFloat    (key, ff)               ->  Arb.shrink ff |> Seq.map (fun ff2 -> IncrByFloat (key, ff2) )
    |MGet           (key, keys)             ->  query{  for xs in Arb.shrink (key::keys) do     // DRY violation in these query expressions?
                                                        where (not (List.isEmpty xs))
                                                        yield MGet (xs.Head, xs.Tail) }
    |MSet           (keyBs, keyBss)         ->  query{  for xs in Arb.shrink (keyBs::keyBss) do
                                                        where (not (List.isEmpty xs))
                                                        yield MSet (xs.Head, xs.Tail) }
    |MSetNX         (keyBs, keyBss)         ->  query{  for xs in Arb.shrink (keyBs::keyBss) do
                                                        where (not (List.isEmpty xs))
                                                        yield MSetNX (xs.Head, xs.Tail) }
    |Set            (key, bs)               ->  Arb.shrink bs |> Seq.map (fun bs2 -> Set (key, bs2) )
    |SetBit         (key, uii, bb)          ->  Arb.shrink uii |> Seq.map (fun uii2 -> SetBit (key, uii2, bb) )
    |SetNX          (key, bs)               ->  Arb.shrink bs |> Seq.map (fun bs2 -> SetNX (key, bs2) )
    |SetRange       (key, uii, bs)          ->  Arb.shrink (uii, bs) |> Seq.map (fun (uii2, bs2) -> SetRange (key, uii2, bs2))
    |Strlen         _                       ->  Seq.empty
    |FlushDB                                ->  Seq.empty
    |Ping                                   ->  Seq.empty




let genFredisCmdList = Gen.nonEmptyListOf Arb.generate<FredisCmd>
let shrinkFredisCmdList (xs:FredisCmd list) = Arb.shrink xs  // stack overflow once registered?                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       



type ArbOverrides() =
    static member Float() =
        Arb.Default.Float()
        |> Arb.filter (fun f -> not <| System.Double.IsNaN(f) && 
                                not <| System.Double.IsInfinity(f) &&
                                (System.Math.Abs(f) < (System.Double.MaxValue / 2.0)) &&
                                (System.Math.Abs(f) > 0.00001 ) )

    static member Key() = Arb.fromGen genKey
    static member ByteOffsets() = Arb.fromGenShrink (genByteOffset, shrinkByteOffset )
//    static member Bytes() = Arb.fromGen genAlphaByteArray
    static member FredisCmd() = Arb.fromGenShrink (genFredisCmd,  shrinkFredisCmd) 
                                |> Arb.filter FredisCmdFilterNoNumOps




Arb.register<ArbOverrides>() |> ignore




let host = "127.0.0.1"
let redisPort = 6380
let fredisPort = 6379


let private sendReceive (client:TcpClient) (msg:Resp) =
    let strm = client.GetStream()
    RespStreamFuncs.AsyncSendResp strm msg |> Async.RunSynchronously
    let aa = 
        async{
            let buf = Array.zeroCreate 1
            let! optRespTypeByte = strm.AsyncReadByte3 buf
            let reply = 
                match optRespTypeByte with
                | None              ->  None
                | Some respTypeByte -> 
                        let respTypeInt = System.Convert.ToInt32(respTypeByte)
                        let respMsg = RespMsgProcessor.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm
                        Some respMsg
            return reply
        }

    aa |> Async.RunSynchronously





// |@ is the fscheck 'property labelling to the left' operator
let (.=.) left right = left = right |@ sprintf "\n%A =\n%A" left right


let mutable ctr = 0

let redisClient     = new TcpClient(host, redisPort)
let fredisClient    = new TcpClient(host, fredisPort)

let propFredisVsRedis (cmds:FredisTypes.FredisCmd list) =

    ctr <- ctr + 1
    if (ctr % 1000) = 0 then
        printfn "test num: %d" ctr

    // not restarting fredis and redis, so the first command is always a flush
    let respCmds = (FlushDB :: cmds) |> List.map (FredisCmdToResp.FredisCmdToRESP >> FredisTypes.Resp.Array)
    let fredisReplies = respCmds |> List.map (sendReceive fredisClient)
    let redisReplies  = respCmds |> List.map (sendReceive redisClient) 
    redisReplies .=. fredisReplies





let propFredisVsRedisNewConnection (cmds:FredisTypes.FredisCmd list) =

    ctr <- ctr + 1
    if (ctr % 250) = 0 then
        printfn "test num: %d - cmds len: %d" ctr cmds.Length

    use redisClientNew     = new TcpClient(host, redisPort)
    use fredisClientNew    = new TcpClient(host, fredisPort)

    // not restarting fredis and redis, so the first command is always a flush
    let respCmds = (FlushDB :: cmds) |> List.map (FredisCmdToResp.FredisCmdToRESP >> FredisTypes.Resp.Array)
    let fredisReplies = respCmds |> List.map (sendReceive fredisClientNew)
    let redisReplies  = respCmds |> List.map (sendReceive redisClientNew) 
    redisReplies .=. fredisReplies


let propFredisVsRedisNewConnectionShrinkGif (cmds:FredisTypes.FredisCmd list) =
    // not restarting fredis and redis, so the first command is always a flush
    let respCmds = (FlushDB :: cmds) |> List.map (FredisCmdToResp.FredisCmdToRESP >> FredisTypes.Resp.Array)
    let fredisReplies = respCmds |> List.map (sendReceive fredisClient)
    let redisReplies  = respCmds |> List.map (sendReceive redisClient) 
    let ok = redisReplies = fredisReplies
//    if not ok then
////        System.Console.Clear()
//        printfn "####: %A\n%A\n%A" cmds fredisReplies redisReplies
//    ok
    //redisReplies .=. fredisReplies
    ok




let lenPreCond xs = (List.length xs) > 1

let propFredisVsRedisWithPreCond  (cmds:FredisTypes.FredisCmd list) =
     lenPreCond cmds ==> lazy propFredisVsRedisNewConnectionShrinkGif cmds


let config = {  Config.Default with 
                    MaxFail = 1000000
                    MaxTest = 100000 }

//Check.Verbose propFredisVsRedis

Check.One (config, propFredisVsRedisWithPreCond)

//printfn "press 'X' to exit"
//
//let rec WaitForExitCmd () = 
//    match System.Console.ReadKey().KeyChar with
//    | 'X'   -> ()
//    | _     -> WaitForExitCmd ()

//WaitForExitCmd ()

