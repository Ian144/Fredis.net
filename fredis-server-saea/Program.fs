open System
open System.Net
open System.Net.Sockets
open System.Collections.Concurrent
open SocAsyncEventArgFuncs


let host = "0.0.0.0"
let port = 6379

// for responding to 'raw' non-RESP pings
[<Literal>]
let PingL = 80  // P - redis-benchmark PING_INLINE just sends PING\r\n, not encoded as RESP

let pongBytes  = "+PONG\r\n"B


let HandleSocketError (name:string) (ex:System.Exception) =
    let rec handleExInner (ex:System.Exception) =
        let msg = ex.Message
        let optInnerEx = FSharpx.FSharpOption.ToFSharpOption ex.InnerException

        match optInnerEx with
        | None  ->          msg
        | Some innerEx ->   let innerMsg = handleExInner innerEx
                            sprintf "%s | %s" msg innerMsg

    let msg = handleExInner ex

    // Microsoft redis-benchmark does not close its socket connections down cleanly
    if not (msg.Contains("forcibly closed")) then
        printfn "%s --> %s" name msg

let ClientError ex =  HandleSocketError "client error" ex
let ConnectionListenerError ex = HandleSocketError "connection listener error" ex



let maxNumConnections = 1
let saeaBufSize = SocAsyncEventArgFuncs.saeaBufSize
let saeaSharedBuffer = Array.zeroCreate<byte> (maxNumConnections * saeaBufSize)
let saeaPool = new ConcurrentStack<SocketAsyncEventArgs>()

for ctr = 0 to (maxNumConnections - 1) do
    let saea   = new SocketAsyncEventArgs()
    let offset = ctr*saeaBufSize
    saea.SetBuffer(saeaSharedBuffer, offset, saeaBufSize)
    saea.add_Completed (fun _ b -> SocAsyncEventArgFuncs.OnClientIOCompleted b)
    saeaPool.Push(saea)



let ClientListenerLoop (bufSize:int) (client:TcpClient) =
    use client = client // without this Dispose would not be called on client
    use netStrm = client.GetStream() // required to write error msg if an SAEA is not available

    match saeaPool.TryPop() with
    | true, saea    ->
        client.NoDelay  <- true // disable Nagles algorithm, don't want small messages to be held back for buffering

        let userTok = {
            Socket = client.Client
            Tcs = null
            ClientBuf = null
            ClientBufPos = Int32.MaxValue
            SaeaBufStart = saeaBufSize   // setting start and end indexes to 1 past the end of the buffer to indicate there is nothing to read
            SaeaBufEnd = saeaBufSize     // as comment above
            }

        saea.UserToken <- userTok

        let buf1 = Array.zeroCreate 1
        let buf5 = Array.zeroCreate 5   // used to eat PONG msgs

        let saeaSrc     = SaeaStreamSource saea :> IFredisStreamSource  
        let saeaSink    = SaeaStreamSink saea   :> IFredisStreamSink

        let mutable firstTime = false

        let asyncProcessClientRequests =
            async{
                while (client.Connected ) do

                    if firstTime then 
                        firstTime <- false
                        let respTypeInt = netStrm.ReadByte() 
                        //let! _ = SocAsyncEventArgFuncs.AsyncRead saea buf1  // todo: does " let! _ = AsyncRead" handle client disconnections, where the Task inside the Async has been cancelled
                        //let respTypeInt = System.Convert.ToInt32(buf1.[0])

                        if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                            // todo: could manually adjust the saea userToken to eat 5 chars
                            let! _ = SocAsyncEventArgFuncs.AsyncRead saea buf5        // todo: let! _ is ugly, fix
                            do! SocAsyncEventArgFuncs.AsyncWrite saea pongBytes
                            ()
                        else
                            let! respMsg = SaeaAsyncRespMsgParser.LoadRESPMsg respTypeInt saeaSrc
                            let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                            match choiceFredisCmd with
                            | Choice1Of2 cmd    ->  let! resp = CmdProcChannel.MailBoxChannel cmd // to process the cmd on a single thread
                                                    do! SaeaAsyncRespStreamFuncs.AsyncSendResp saeaSink resp
                                                    do! saeaSink.AsyncFlush ()
                            | Choice2Of2 err    ->  do! SaeaAsyncRespStreamFuncs.AsyncSendError saeaSink err
                                                    do! saeaSink.AsyncFlush ()
                        
                    else
                        let! _ = SocAsyncEventArgFuncs.AsyncRead saea buf1  // todo: does " let! _ = AsyncRead" handle client disconnections, where the Task inside the Async has been cancelled
                        let respTypeInt = System.Convert.ToInt32(buf1.[0])

                        if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                            // todo: could manually adjust the saea userToken to eat 5 chars
                            let! _ = SocAsyncEventArgFuncs.AsyncRead saea buf5        // todo: let! _ is ugly, fix
                            do! SocAsyncEventArgFuncs.AsyncWrite saea pongBytes
                            ()
                        else
                            let! respMsg = SaeaAsyncRespMsgParser.LoadRESPMsg respTypeInt saeaSrc
                            let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                            match choiceFredisCmd with
                            | Choice1Of2 cmd    ->  let! resp = CmdProcChannel.MailBoxChannel cmd // to process the cmd on a single thread
                                                    do! SaeaAsyncRespStreamFuncs.AsyncSendResp saeaSink resp
                                                    do! saeaSink.AsyncFlush ()
                            | Choice2Of2 err    ->  do! SaeaAsyncRespStreamFuncs.AsyncSendError saeaSink err
                                                    do! saeaSink.AsyncFlush ()

            }

        Async.StartWithContinuations(
                asyncProcessClientRequests,
                (fun () ->  client.Dispose()
                            saeaPool.Push saea),
                (fun ex ->  client.Dispose() 
                            saeaPool.Push saea
                            ClientError ex),
                (fun ct ->  client.Dispose() 
                            saeaPool.Push saea
                            printfn "ClientListener cancelled: %A" ct)
        )


    | false, xx  ->  netStrm.Write(ErrorMsgs.maxNumClientsReached, 0, ErrorMsgs.maxNumClientsReached.Length)





let ConnectionListenerLoop (bufSize:int) (listener:TcpListener) =
    let asyncConnectionListener =
        async {
            while true do
                let acceptClientTask = listener.AcceptTcpClientAsync ()
                let! client = Async.AwaitTask acceptClientTask
                do ClientListenerLoop bufSize client
        }

    Async.StartWithContinuations(
        asyncConnectionListener,
        (fun _  -> printfn "ConnectionListener completed"),
        ConnectionListenerError,
        (fun ct -> printfn "ConnectionListener cancelled: %A" ct)
    )



let WaitForExitCmd () =
    while System.Console.ReadKey().KeyChar <> 'X' do
        ()



//-------------------------------------------


let ClientListenerLoop2 (client:Socket, saea:SocketAsyncEventArgs) : unit=

//    use client = client // without this Dispose would not be called on client
    client.NoDelay  <- true // disable Nagles algorithm, don't want small messages to be held back for buffering

    let userTok = {
        Socket = client
        Tcs = null
        ClientBuf = null
        ClientBufPos = Int32.MaxValue
        SaeaBufStart = saeaBufSize   // setting start and end indexes to 1 past the end of the buffer to indicate there is nothing to read
        SaeaBufEnd = saeaBufSize     // as comment above
        }

    saea.UserToken <- userTok

    let buf1 = Array.zeroCreate 1
    let buf5 = Array.zeroCreate 5   // used to eat PONG msgs

    let saeaSrc     = SaeaStreamSource saea :> IFredisStreamSource  
    let saeaSink    = SaeaStreamSink saea   :> IFredisStreamSink

    let asyncProcessClientRequests = 
        async{ 
            while (client.Connected ) do
                let! _ = SocAsyncEventArgFuncs.AsyncRead saea buf1  // todo: does " let! _ = AsyncRead" handle client disconnections, where the Task inside the Async has been cancelled
                let respTypeInt = System.Convert.ToInt32(buf1.[0])

                if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                    // todo: could manually adjust the saea userToken to eat 5 chars
                    let! _ = SocAsyncEventArgFuncs.AsyncRead saea buf5        // todo: let! _ is ugly, fix
                    do! SocAsyncEventArgFuncs.AsyncWrite saea pongBytes
                    ()
                else
                    let! respMsg = SaeaAsyncRespMsgParser.LoadRESPMsg respTypeInt saeaSrc
                    let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                    match choiceFredisCmd with
                    | Choice1Of2 cmd    ->  let! resp = CmdProcChannel.MailBoxChannel cmd // to process the cmd on a single thread
                                            do! SaeaAsyncRespStreamFuncs.AsyncSendResp saeaSink resp
                                            do! saeaSink.AsyncFlush ()
                    | Choice2Of2 err    ->  do! SaeaAsyncRespStreamFuncs.AsyncSendError saeaSink err
                                            do! saeaSink.AsyncFlush ()
            }

    Async.StartWithContinuations(
            asyncProcessClientRequests,
            (fun () -> saeaPool.Push saea),
            (fun ex -> saeaPool.Push saea
                       ClientError ex),
            (fun ct -> saeaPool.Push saea
                       printfn "ClientListener cancelled: %A" ct)
        ) // end Async





let ProcessAccept (saeaAccept:SocketAsyncEventArgs) = 
    match saeaPool.TryPop() with
    | true, saea    ->  ClientListenerLoop2(saeaAccept.AcceptSocket, saea)
    | false, _      ->  ()//todo: send connection failure msg to client


let StartAccept (listenSocket:Socket) (acceptEventArg:SocketAsyncEventArgs) =
    acceptEventArg.AcceptSocket <- null
    let ioPending = listenSocket.AcceptAsync acceptEventArg
    if not ioPending then
        ProcessAccept acceptEventArg


// see C:\Users\Ian\Documents\GitHub\suave\src\Suave\Tcp.fs (49)


[<EntryPoint>]
let main argv =

    let cBufSize =
        if argv.Length = 1 then
            Utils.ChoiceParseInt (sprintf "invalid integer %s" argv.[0]) argv.[0]
        else
            Choice1Of2 (8 * 1024)

    match cBufSize with
    | Choice1Of2 bufSize ->
        printfn "buffer size: %d"  bufSize

//        let addressList = Dns.GetHostEntry(Environment.MachineName).AddressList
//        let ipAddr = addressList.[addressList.Length - 1]
        let ipAddr = IPAddress.Parse(host)
        let localEndPoint = IPEndPoint (ipAddr, port)
        use listenSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)

//        if localEndPoint.AddressFamily = AddressFamily.InterNetworkV6 then 
//            listenSocket.SetSocketOption (SocketOptionLevel.IPv6, SocketOptionName.IPv6Only, false) 
//            listenSocket.Bind(new IPEndPoint(IPAddress.IPv6Any, localEndPoint.Port));            
//        else
//            listenSocket.Bind(localEndPoint)

        listenSocket.Bind(localEndPoint)


        listenSocket.Listen 1

        let acceptEventArg = new SocketAsyncEventArgs();
        acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
        StartAccept listenSocket acceptEventArg

        WaitForExitCmd ()
        printfn "stopped"
        0


//        let ipAddr = IPAddress.Parse(host)
//        let listener = TcpListener(ipAddr, port)
//        listener.Start ()
//        ConnectionListenerLoop bufSize listener
//        printfn "fredis startup complete\nawaiting incoming connection requests\npress 'X' to exit"
//        WaitForExitCmd ()
//        do Async.CancelDefaultToken()
//        printfn "cancelling asyncs"
//        listener.Stop()
//        printfn "stopped"
//        0 // return an integer exit code
    | Choice2Of2 msg -> printf "%s" msg
                        1 // non-zero exit code

