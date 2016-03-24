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





let maxNumConnections = 1024
let saeaBufSize = SocAsyncEventArgFuncs.saeaBufSize
let saeaSharedBuffer = Array.zeroCreate<byte> (maxNumConnections * saeaBufSize)
let saeaPool = new ConcurrentStack<SocketAsyncEventArgs>()

for ctr = 0 to (maxNumConnections - 1) do
    let saea = new SocketAsyncEventArgs()
    let offset = ctr*saeaBufSize
    saea.SetBuffer(saeaSharedBuffer, offset, saeaBufSize)
    saea.Completed.Subscribe  SocAsyncEventArgFuncs.OnClientIOCompleted  |> ignore
    saeaPool.Push(saea)





let ClientListenerLoop (bufSize:int) (client:TcpClient) =
    use client = client // without this Dispose would not be called on client
    use netStrm = client.GetStream()

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

        let saeaSrc = SaeaStreamSource saea
        let saeaSink = SaeaStreamSink saea


        let asyncProcessClientRequests =
            async{
                // msdn: "It is assumed that you will almost always be doing a series of reads or writes, but rarely alternate between the two of them"
                // Fredis does alternate between reads and writes, but tests have shown that BufferedStream still gives a perf boost without errors
                // BufferedStream will deadlock if there are simultaneous async reads and writes in progress, due to an internal semaphore. But works if this is not the case.
                // The F# async workflow sequences async reads and writes so none are simultaneous.
                use strm = new System.IO.BufferedStream ( netStrm, bufSize )
                while (client.Connected ) do
                    let! _ = SocAsyncEventArgFuncs.AsyncRead saea buf1  // todo: does " let! _ = AsyncRead" handle client disconnections, where the Task inside the Async has been cancelled
                    let respTypeInt = System.Convert.ToInt32(buf1.[0])
                    if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                        // todo: could manually adjust the saea userToken to eat 5 chars
                        let! _ = SocAsyncEventArgFuncs.AsyncRead saea buf5        // todo: let! _ is ugly, fix
                        do! SocAsyncEventArgFuncs.AsyncWrite saea pongBytes 
                        ()
                    else
//                        let respMsg = RespMsgParser.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm
                        //let! respMsg = AsyncRespMsgParser.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm
                        let! respMsg = SaeaAsyncRespMsgParser.LoadRESPMsg respTypeInt saeaSrc
                        let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                        match choiceFredisCmd with
                        | Choice1Of2 cmd    ->  let! resp = CmdProcChannel.MailBoxChannel cmd // to process the cmd on a single thread
                                                do! AsyncRespStreamFuncs.AsyncSendResp saeaSink resp
//                                                do! RespStreamFuncs.AsyncSendResp strm resp
                                                do! strm.FlushAsync() |> Async.AwaitTask
                        | Choice2Of2 err    ->  do! RespStreamFuncs.AsyncSendError strm err
                                                do! strm.FlushAsync() |> Async.AwaitTask
            }

        Async.StartWithContinuations(
                asyncProcessClientRequests,
                (fun () -> saeaPool.Push saea),
                (fun ex -> saeaPool.Push saea
                           ClientError ex),
                (fun ct -> saeaPool.Push saea
                           printfn "ClientListener cancelled: %A" ct)
        )


    | false, _  ->  netStrm.Write(ErrorMsgs.maxNumClientsReached, 0, ErrorMsgs.maxNumClientsReached.Length)





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

        let ipAddr = IPAddress.Parse(host)
        let listener = TcpListener(ipAddr, port)
        listener.Start ()
        ConnectionListenerLoop bufSize listener
        printfn "fredis startup complete\nawaiting incoming connection requests\npress 'X' to exit"
        WaitForExitCmd ()
        do Async.CancelDefaultToken()
        printfn "cancelling asyncs"
        listener.Stop()
        printfn "stopped"
        0 // return an integer exit code
    | Choice2Of2 msg -> printf "%s" msg
                        1 // non-zero exit code

