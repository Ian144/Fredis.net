
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
let genKey = Gen.frequency[(1, key1); (1, key2); (1, key3); (1, key4); (1, key5); (1, key6); (1, key7); (1, key8) ]


// create an Arbitrary<ByteOffset> so as to avoid the runtime error below
// "The type FredisTypes+ByteOffset is not handled automatically by FsCheck. Consider using another type or writing and registering a generator for it"

let private maxByteOffset = (pown 2 29) - 1 // zero based, hence the -1
let private minByteOffset = (pown 2 29) * -1 


let genByteOffset = 
    Gen.choose(minByteOffset, maxByteOffset)
    |> Gen.map FredisTypes.ByteOffset.Create
    |> Gen.map (fun optBoffset -> optBoffset.Value)

let shrinkByteOffset (bo:FredisTypes.ByteOffset) = 
    let optBo2 = FredisTypes.ByteOffset.Create (bo.Value / 2)
    let bo2 = optBo2.Value
    seq{ yield bo2 }


let genAlphaByte = Gen.choose(65,90) |> Gen.map byte 
let genAlphaByteArray = Gen.arrayOfLength 16 genAlphaByte 




// overrides created to apply too nested values in reflexively generated types
type Overrides() =
    static member Float() =
        Arb.Default.Float()
        |> Arb.filter (fun f -> not <| System.Double.IsNaN(f) && 
                                not <| System.Double.IsInfinity(f) &&
                                (System.Math.Abs(f) < (System.Double.MaxValue / 2.0)) &&
                                (System.Math.Abs(f) > 0.00001 ) )

    static member Key() = Arb.fromGen genKey
    static member ByteOffsets() = Arb.fromGenShrink (genByteOffset, shrinkByteOffset )
    static member Bytes() = Arb.fromGen genAlphaByteArray
    




Arb.register<Overrides>() |> ignore




let host = "127.0.0.1"
let redisPort = 6380
let fredisPort = 6379

let flushDBCmd = "FLUSHDB" |> (Utils.StrToBytes >> BulkStrContents.Contents >> Resp.BulkString)
let arFlushDBCmd = FredisTypes.Resp.Array [| flushDBCmd |]


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
    let reply = aa |> Async.RunSynchronously
    reply



let propFredisVsRedis (cmdsIn:FredisTypes.FredisCmd list) =
    
    let cmds = cmdsIn |> List.filter (fun cmd ->    match cmd with
                                                    | BitOp _       -> false
                                                    | Bitcount _    -> false
//                                                    | SetRange _    -> false
//                                                    | GetRange _    -> false
                                                    | IncrByFloat _ -> false
                                                    | Bitpos _      -> false
                                                    | IncrBy _      -> false
                                                    | Incr _        -> false
                                                    | Decr _        -> false
                                                    | DecrBy _      -> false
                                                    | FlushDB _     -> false
                                                    | _             -> true)


    let respCmds = cmds |> List.map (FredisCmdToResp.FredisCmdToRESP >> FredisTypes.Resp.Array)
    let respCmds2 = arFlushDBCmd :: respCmds // flush live redis and fredis instances before running other cmds

    use redisClient     = new TcpClient(host, redisPort)
    use fredisClient    = new TcpClient(host, fredisPort)
    let fredisReplies = respCmds2 |> List.map (sendReceive fredisClient)
    let redisReplies  = respCmds2 |> List.map (sendReceive redisClient) 

    let ok = redisReplies = fredisReplies
//    if not ok then
//        let xs = List.zip3 respCmds2 redisReplies fredisReplies
//        printfn "-------------------------------------------------------------------- begin"
//        xs |> List.iter (printfn "%A")
//        printfn "-------------------------------------------------------------------- end"

    ok



let config = { FsCheck.Config.Default with MaxTest = 10000 }
//Check.Quick propFredisVsRedis
Check.One (config, propFredisVsRedis)

printfn "press any key to exit"
System.Console.ReadKey |> ignore
System.Console.ReadKey |> ignore


