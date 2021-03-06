﻿open System
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
    if not (msg.Contains("forcibly closed") || msg.Contains("ConnectionReset")) then
        printfn "%s --> %s" name msg


let ClientError ex =  HandleSocketError "client error" ex
let ConnectionListenerError ex = HandleSocketError "connection listener error" ex



let maxNumConnections = 1024
let saeaBufSize = 1024 * 32
let saeaSharedBuffer = Array.zeroCreate<byte> (maxNumConnections * saeaBufSize)
let saeaPool = new ConcurrentStack<SocketAsyncEventArgs>()

for ctr = 0 to (maxNumConnections - 1) do
    let saea   = new SocketAsyncEventArgs()
    let offset = ctr*saeaBufSize
    saea.SetBuffer(saeaSharedBuffer, offset, saeaBufSize)
    saea.add_Completed (fun _ b -> SocAsyncEventArgFuncs.OnClientIOCompleted b)
    saeaPool.Push(saea)






let ClientListenerLoop (client:Socket, saea:SocketAsyncEventArgs) : unit =

    let userTok:UserToken = {
        Socket = client
        ClientBuf = null
        ClientBufPos = Int32.MaxValue
        SaeaBufStart = saeaBufSize   // setting start and end indexes to 1 past the end of the buffer indicates there is nothing to read
        SaeaBufEnd = saeaBufSize     // as comment above
        SaeaBufSize = saeaBufSize
        Continuation = -1
        BufList = Collections.Generic.List<byte[]>() //todo, can this be null
        okContBytes = ignore
        okContUnit = ignore
        exnCont = ignore
        cancCont = ignore
        }

    saea.UserToken <- userTok

    // pre-created arrays to avoid allocation for each cmd, each client connection has its own instances
    let buf1 = Array.zeroCreate 1   // used to when waiting on a new msg
    let buf5 = Array.zeroCreate 5   // used to eat PONG msgs

    // consider F# anonymous classes
    let saeaSrc     = SaeaStreamSource saea :> ISaeaStreamSource  
    let saeaSink    = SaeaStreamSink saea   :> ISaeaStreamSink


    let asyncProcessClientRequests = 
        async{ 
            use client = client //so the client is closed when this async action exitss
            while client.Connected do
                let! bb = SocAsyncEventArgFuncs.AsyncReadByte2 saea buf1
                let respTypeInt = System.Convert.ToInt32 bb
                if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                    let! _ = SocAsyncEventArgFuncs.AsyncRead saea buf5                     // todo: could manually adjust the saea userToken to eat 5 chars
                    SocAsyncEventArgFuncs.Reset saea
                    do! SocAsyncEventArgFuncs.AsyncWrite saea pongBytes
                    do! saeaSink.AsyncFlush ()
                    SocAsyncEventArgFuncs.Reset saea
                else
                    let! respMsg = SaeaAsyncRespMsgParser.LoadRESPMsg respTypeInt saeaSrc
                    SocAsyncEventArgFuncs.Reset saea
                    let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                    match choiceFredisCmd with
                    | Choice1Of2 cmd    ->  let! reply = CmdProcChannel.MailBoxChannel cmd  // to process the cmd on a single thread
                                            SocAsyncEventArgFuncs.Reset saea
                                            do! SaeaAsyncRespStreamFuncs.AsyncSendResp saeaSink reply
                                            do! saeaSink.AsyncFlush ()
                                            SocAsyncEventArgFuncs.Reset saea
                    | Choice2Of2 err    ->  SocAsyncEventArgFuncs.Reset saea
                                            do! SaeaAsyncRespStreamFuncs.AsyncSendError saeaSink err
                                            do! saeaSink.AsyncFlush ()
                                            SocAsyncEventArgFuncs.Reset saea
            client.Shutdown SocketShutdown.Both
        }

    Async.StartWithContinuations(
            asyncProcessClientRequests,
            (fun () ->  saeaPool.Push saea),
            (fun ex ->  saeaPool.Push saea
                        ClientError ex),
            (fun _  ->  saeaPool.Push saea) )



let rec ProcessAccept (saeaAccept:SocketAsyncEventArgs) = 
    let listenSocket = saeaAccept.UserToken :?> Socket
    match saeaPool.TryPop() with
    | true, saea    ->  ClientListenerLoop(saeaAccept.AcceptSocket, saea)
    | false, _      ->  use clientSocket = saeaAccept.AcceptSocket
                        clientSocket.Shutdown SocketShutdown.Both
                        clientSocket.Send ErrorMsgs.maxNumClientsReached |> ignore
    StartAccept listenSocket saeaAccept
and StartAccept (listenSocket:Socket) (acceptEventArg:SocketAsyncEventArgs) =
    acceptEventArg.AcceptSocket <- null
    let ioPending = listenSocket.AcceptAsync acceptEventArg
    if not ioPending then
        ProcessAccept acceptEventArg


let OnAcceptCompleted _ (saea:SocketAsyncEventArgs) = 
    ProcessAccept saea

let eventHandler = EventHandler<SocketAsyncEventArgs>( OnAcceptCompleted )



let WaitForExitCmd () = 
    while stdin.Read() <> 88 do // 88 is 'X'
        ()


[<EntryPoint>]
let main argv =


    let cBufSize =
        if argv.Length = 1 then
            Utils.ChoiceParseInt (sprintf "invalid integer %s" argv.[0]) argv.[0]
        else
            Choice1Of2 (65 * 1024)

    match cBufSize with
    |   Choice1Of2 bufSize ->
            printfn "buffer size: %d" bufSize
            let ipAddr = IPAddress.Parse(host)
            let localEndPoint = IPEndPoint (ipAddr, port)
            use listenSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            listenSocket.Bind localEndPoint
            listenSocket.Listen maxNumConnections

            let acceptEventArg = new SocketAsyncEventArgs()
            acceptEventArg.UserToken <- listenSocket
            acceptEventArg.add_Completed eventHandler

            StartAccept listenSocket acceptEventArg
            WaitForExitCmd ()
            printfn "stopped"
            0

    | Choice2Of2 msg -> 
            printf "%s" msg
            1 // non-zero exit code

