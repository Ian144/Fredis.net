﻿open System
open System.Net
open System.Net.Sockets
open System.Collections.Concurrent
open SocAsyncEventArgFuncs


open FredisTypes
open RespStreamFuncs
open FsCheck

let host = "127.0.0.1"
let port = 6379

// for responding to 'raw' non-RESP pings
[<Literal>]
let PingL = 80  // P - redis-benchmark PING_INLINE just sends PING\r\n, not encoded as RESP

let pongBytes  = "+PONG\r\n"B




let maxNumConnections = 1024
let saeaBufSize = 8 // * 64
let saeaSharedBuffer = Array.zeroCreate<byte> (maxNumConnections * saeaBufSize)
let saeaPool = new ConcurrentStack<SocketAsyncEventArgs>()






let ClientListenerLoop (client:Socket, saea:SocketAsyncEventArgs) : unit =
//    use client = client // without this Dispose would not be called on client, todo: did this cause an issue - the socket was disposed to early
    client.NoDelay  <- true // disable Nagles algorithm, don't want small messages to be held back for buffering

    let userTok:UserToken = {
        Socket = client
        Tcs = null
        ClientBuf = null
        ClientBufPos = Int32.MaxValue
        SaeaBufStart = 0
        SaeaBufEnd = 0
        SaeaBufSize = saeaBufSize
        SaeaBufOffset = saea.Offset
        Continuation = ignore
        BufList = Collections.Generic.List<byte[]>()
        }

    saea.UserToken <- userTok
    let buf5 = Array.zeroCreate 5   // used to eat PONG msgs

    // todo: consider F# anonymous classes for implenting IFredisStreamSource and IFredisStreamSink
    let saeaSrc     = SaeaStreamSource saea :> IFredisStreamSource  
    let saeaSink    = SaeaStreamSink saea   :> IFredisStreamSink

    let asyncProcessClientRequests = 
        async{ 
            while (client.Connected ) do
                let! bb = SocAsyncEventArgFuncs.AsyncReadByte saea
                let respTypeInt = System.Convert.ToInt32 bb
                if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                    // todo: could manually adjust the saea userToken to eat 5 chars
                    let! _ = SocAsyncEventArgFuncs.AsyncRead saea buf5        // todo: let! _ is ugly, fix
                    SocAsyncEventArgFuncs.Reset saea
                    do! SocAsyncEventArgFuncs.AsyncWrite saea pongBytes
                    do! saeaSink.AsyncFlush ()
                    SocAsyncEventArgFuncs.Reset saea
                    ()
                else
                    let! respMsg = SaeaAsyncRespMsgParser.LoadRESPMsg respTypeInt saeaSrc
                    SocAsyncEventArgFuncs.Reset saea
                    let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                    match choiceFredisCmd with
                    | Choice1Of2 cmd    ->  let! reply = CmdProcChannel.MailBoxChannel cmd 
                                            SocAsyncEventArgFuncs.Reset saea
                                            do! SaeaAsyncRespStreamFuncs.AsyncSendResp saeaSink reply
                                            do! saeaSink.AsyncFlush ()
                                            SocAsyncEventArgFuncs.Reset saea
                    | Choice2Of2 err    ->  SocAsyncEventArgFuncs.Reset saea
                                            do! SaeaAsyncRespStreamFuncs.AsyncSendError saeaSink err
                                            do! saeaSink.AsyncFlush ()
                                            SocAsyncEventArgFuncs.Reset saea
            }

    Async.StartWithContinuations(
            asyncProcessClientRequests,
            (fun () -> saeaPool.Push saea),
            (fun ex -> saeaPool.Push saea
                       printfn "%A" ex),
            (fun ct -> saeaPool.Push saea
                       printfn "ClientListener cancelled: %A" ct)
        ) // end Async




let rec ProcessAccept (saeaAccept:SocketAsyncEventArgs) = 
    let listenSocket = saeaAccept.UserToken :?> Socket
    match saeaPool.TryPop() with
    | true, saea    ->  let clientSoc = saeaAccept.AcceptSocket
                        ClientListenerLoop(saeaAccept.AcceptSocket, saea)
    | false, _      ->  use clientSocket = saeaAccept.AcceptSocket
                        clientSocket.Send ErrorMsgs.maxNumClientsReached |> ignore
                        clientSocket.Disconnect false
//                        clientSocket.Close()
    StartAccept listenSocket saeaAccept
and StartAccept (listenSocket:Socket) (acceptEventArg:SocketAsyncEventArgs) =
    try
        acceptEventArg.AcceptSocket <- null
        let ioPending = listenSocket.AcceptAsync acceptEventArg
        if not ioPending then
            ProcessAccept acceptEventArg
    with
        | :? System.ObjectDisposedException ->  // printfn "object disposed exception in StartAccept"
                                                ()






let private sendReceive (client:TcpClient) (msg:Resp) =
    printfn "\n\n-------"
    printfn "client sending: %A" msg
    let strm = client.GetStream()
    RespStreamFuncs.AsyncSendResp strm msg |> Async.RunSynchronously
    let respTypeInt = strm.ReadByte()
    RespMsgParser.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm




let key1 = Gen.constant (Key "key1")
let key2 = Gen.constant (Key "key2")
let key3 = Gen.constant (Key "key3")
let key4 = Gen.constant (Key "key4")
let key5 = Gen.constant (Key "key5")
let key6 = Gen.constant (Key "key6")
let key7 = Gen.constant (Key "key7")
let key8 = Gen.constant (Key "key8")
let genKey = Gen.frequency[(1, key1); (1, key2); (1, key3); (1, key4); (1, key5); (1, key6); (1, key7); (1, key8) ]


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
    |MGet           (key, keys)             ->  let mgets = query{  for xs in Arb.shrink (key::keys) do
                                                                    where (not (List.isEmpty xs))
                                                                    yield MGet (xs.Head, xs.Tail)   } 
                                                let gets = (key::keys) |> List.map Get // consider Get to be a 'shrunk' MGet
                                                seq{ yield! gets; yield! mgets } 
    |MSet           (keyBs, keyBss)         ->  let msets = query{  for xs in Arb.shrink (keyBs::keyBss) do
                                                                    where (not (List.isEmpty xs))
                                                                    yield MSet (xs.Head, xs.Tail) }
                                                let sets = (keyBs::keyBss) |> List.map Set // consider Set to be a 'shrunk' MSet
                                                seq{ yield! sets; yield! msets } 
    |MSetNX         (keyBs, keyBss)         ->  let msetnxs =   query{  for xs in Arb.shrink (keyBs::keyBss) do
                                                                        where (not (List.isEmpty xs))
                                                                        yield MSetNX (xs.Head, xs.Tail) }
                                                let sets = (keyBs::keyBss) |> List.map Set // consider Set to be a 'shrunk' MSetNX
                                                seq{ yield! sets; yield! msetnxs } 
    |Set            (key, bs)               ->  Arb.shrink bs |> Seq.map (fun bs2 -> Set (key, bs2) )
    |SetBit         (key, uii, bb)          ->  Arb.shrink uii |> Seq.map (fun uii2 -> SetBit (key, uii2, bb) )
    |SetNX          (key, bs)               ->  let setNxs  = Arb.shrink bs |> Seq.map (fun bs2 -> SetNX (key, bs2) )
                                                let sets    = Arb.shrink bs |> Seq.map (fun bs2 -> Set (key, bs2) ) // consider Set to be a 'shrunk' MSetNX
                                                seq{yield! sets; yield! setNxs}
    |SetRange       (key, uii, bs)          ->  Arb.shrink (uii, bs) |> Seq.map (fun (uii2, bs2) -> SetRange (key, uii2, bs2))
    |Strlen         _                       ->  Seq.empty
    |FlushDB                                ->  Seq.empty
    |Ping                                   ->  Seq.empty
     
    
//let genAlphaByte = Gen.choose(65,90) |> Gen.map byte 
let genAlphaByte = Gen.choose(88,88) |> Gen.map byte 
let genAlphaByteArray = Gen.arrayOfLength 8 genAlphaByte 
let genFredisCmd = Arb.generate<FredisCmd>

type ArbOverrides() =
    static member Float() =
        Arb.Default.Float()
        |> Arb.filter (fun f -> not <| System.Double.IsNaN(f) && 
                                not <| System.Double.IsInfinity(f) &&
                                (System.Math.Abs(f) < (System.Double.MaxValue / 2.0)) &&
                                (System.Math.Abs(f) > 0.00001 ) )

    static member Key() = Arb.fromGen genKey
    static member ByteOffsets() = Arb.fromGenShrink (genByteOffset, shrinkByteOffset )
    static member Bytes() = Arb.fromGen genAlphaByteArray
    static member FredisCmd() = Arb.fromGenShrink (genFredisCmd,  shrinkFredisCmd) 




Arb.register<ArbOverrides>() |> ignore


//let StartFredis () =
//    async{
//        let ipAddr = IPAddress.Parse(host)
//        let localEndPoint = IPEndPoint (ipAddr, port)
//        use listenSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
//        listenSocket.Bind(localEndPoint)
//        listenSocket.Listen 1
//        let acceptEventArg = new SocketAsyncEventArgs();
//        acceptEventArg.UserToken <- listenSocket
//        acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
//        StartAccept listenSocket acceptEventArg
//        return () }



let mutable ctr = 0

let propRespReceivedIsValid (cmds:FredisTypes.FredisCmd list) =

    saeaPool.Clear()
    for ctr = 0 to (maxNumConnections - 1) do
        let saea   = new SocketAsyncEventArgs()
        let offset = ctr*saeaBufSize
        saea.SetBuffer(saeaSharedBuffer, offset, saeaBufSize)
        saea.add_Completed (fun _ b -> SocAsyncEventArgFuncs.OnClientIOCompleted b)
        saeaPool.Push(saea)


    try
        // start fredis server, calls will be serviced on .net threadpool threads
        let ipAddr = IPAddress.Parse(host)
        let localEndPoint = IPEndPoint (ipAddr, port)
        use listenSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        listenSocket.Bind(localEndPoint)
        listenSocket.Listen 16
        let acceptEventArg = new SocketAsyncEventArgs()
        acceptEventArg.UserToken <- listenSocket
        acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
        StartAccept listenSocket acceptEventArg

        let tcpClient = new TcpClient(host, port)
        let respCmds = (FlushDB :: cmds) |> List.map (FredisCmdToResp.FredisCmdToRESP >> FredisTypes.Resp.Array)
        let fredisReply = respCmds |> List.map (sendReceive tcpClient)

//        listenSocket.Disconnect false

//        acceptEventArg.add_Completed (fun _ saea -> ())
        listenSocket.Shutdown SocketShutdown.Both
        printfn "after listenSocket.Close"

    with
    | :? System.ObjectDisposedException -> ()
    | :? System.Net.Sockets.SocketException -> ()

    true




let config = {  Config.Default with 
                    EveryShrink = (sprintf "%A" )
//                    Replay = Some (Random.StdGen (310046944,296129814))
//                    StartSize = 32
                    MaxFail = 1000
                    MaxTest = 1000 }

//Check.Verbose propFredisVsRedis

Check.One (config, propRespReceivedIsValid)


printfn "tests complete"
System.Console.ReadKey() |> ignore