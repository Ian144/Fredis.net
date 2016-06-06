open System.Net
open System.Net.Sockets

open AsyncRespStreamFuncs



let host = """0.0.0.0"""
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

let bsOk = FredisTypes.BulkString (FredisTypes.BulkStrContents.Contents "OK"B)

let ClientListenerLoop (bufSize:int) (client:TcpClient) =

    client.NoDelay <- false
    client.ReceiveBufferSize    <- bufSize
    client.SendBufferSize       <- bufSize
    
    let buf1 = Array.zeroCreate 1  // pre-allocate a buffer for reading a single byte, that will only be used for this client
    let buf5 = Array.zeroCreate 5  // pre-allocate a buffer for reading a PING byteS, that will only be used for this client

    let asyncProcessClientRequestsFull =
        let mutable loopAgain = true
        async{
            use client = client // without this Dispose would not be called on client
            use netStrm = client.GetStream()

            // msdn re BufferedStreams: "It is assumed that you will almost always be doing a series of reads or writes, but rarely alternate between the two of them"
            // Fredis.net does alternate between reads and writes, but tests have shown that BufferedStream still gives a perf boost without errors
            // BufferedStream will deadlock if there are simultaneous async reads and writes in progress, due to an internal semaphore. But works if this is not the case.
            // The F# async workflow sequences async reads and writes so they are not simultaneous.
            use strm = new System.IO.BufferedStream( netStrm, bufSize )
            while (client.Connected && loopAgain) do
                let! optRespTypeByte = strm.AsyncReadByte buf1 
                match optRespTypeByte with
                | None              ->  loopAgain <- false  // client disconnected
                | Some respTypeByte -> 
                    let respTypeInt = int respTypeByte
                    if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                        let! _ =  strm.AsyncRead (buf5, 0, 5) //todo, pass cancellation token
                        do! strm.AsyncWrite pongBytes
                        do! strm.FlushAsync() |> Async.AwaitTask
                    else
                        let! respMsg = AsyncRespMsgParser.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm
                        let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                        match choiceFredisCmd with 
                        | Choice1Of2 cmd    ->  let! reply = CmdProcChannel.MailBoxChannel cmd // to process the cmd on a single thread
                                                do! AsyncRespStreamFuncs.AsyncSendResp strm reply
                                                do! strm.FlushAsync() |> Async.AwaitTask
                        | Choice2Of2 err    ->  do! AsyncRespStreamFuncs.AsyncSendError strm err
                                                do! strm.FlushAsync() |> Async.AwaitTask
        }

    let asyncProcessClientRequestsSemi =
        let mutable loopAgain = true
        async{
            use client = client // without this Dispose would not be called on client
            use netStrm = client.GetStream()

            // msdn re BufferedStreams: "It is assumed that you will almost always be doing a series of reads or writes, but rarely alternate between the two of them"
            // Fredis.net does alternate between reads and writes, but tests have shown that BufferedStream still gives a perf boost without errors
            // BufferedStream will deadlock if there are simultaneous async reads and writes in progress, due to an internal semaphore. But works if this is not the case.
            // The F# async workflow sequences async reads and writes so they are not simultaneous.
            use strm = new System.IO.BufferedStream( netStrm, bufSize )
            while (client.Connected && loopAgain) do
                let! optRespTypeByte = strm.AsyncReadByte buf1 
                match optRespTypeByte with
                | None              ->  loopAgain <- false  // client disconnected
                | Some respTypeByte -> 
                    let respTypeInt = int respTypeByte
                    if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                        RespStreamFuncs.Eat5NoAlloc strm  
                        strm.Write (pongBytes, 0, pongBytes.Length)
                        do! strm.FlushAsync() |> Async.AwaitTask
                    else
                        let respMsg = RespMsgParser.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm
                        let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                        match choiceFredisCmd with 
                        | Choice1Of2 cmd    ->  let! reply = CmdProcChannel.MailBoxChannel cmd // to process the cmd on a single thread
                                                RespStreamFuncs.SendResp strm reply
                                                do! strm.FlushAsync() |> Async.AwaitTask
                        | Choice2Of2 err    ->  do! AsyncRespStreamFuncs.AsyncSendError strm err
                                                do! strm.FlushAsync() |> Async.AwaitTask
        }




    Async.StartWithContinuations(
         asyncProcessClientRequestsSemi,
         ignore,
         ClientError,
         (fun ct    -> printfn "ClientListener cancelled: %A" ct)
    )

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


#nowarn "52"
let WaitForExitCmd () = 
    while stdin.Read() <> 88 do // 88 is 'X'
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

