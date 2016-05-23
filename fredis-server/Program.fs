﻿open System.Net
open System.Net.Sockets

open RespStreamFuncs

open System.IO


//let host = """127.0.0.1"""
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





let ClientListenerLoop (bufSize:int) (client:TcpClient) =

    client.NoDelay <- true // disable Nagle, don't want small messages to be held back for buffering
    client.ReceiveBufferSize    <- bufSize
    client.SendBufferSize       <- bufSize
    
    let buf = Array.zeroCreate 1 

    let asyncProcessClientRequests =
        let mutable loopAgain = true
        async{
            use client = client // without this Dispose would not be called on client
            use netStrm = client.GetStream()

            // msdn: "It is assumed that you will almost always be doing a series of reads or writes, but rarely alternate between the two of them"
            // Fredis does alternate between reads and writes, but tests have shown that BufferedStream still gives a perf boost without errors
            // BufferedStream will deadlock if there are simultaneous async reads and writes in progress, due to an internal semaphore. But works if this is not the case.
            // The F# async workflow sequences async reads and writes so none are simultaneous.
            use strm = new System.IO.BufferedStream( netStrm, bufSize )
//            let strm = netStrm
            while (client.Connected && loopAgain) do
                // reading from the socket is synchronous after this point, until current redis msg is processed
                let! optRespTypeByte = strm.AsyncReadByte buf 
                match optRespTypeByte with
                | None              ->  loopAgain <- false  // client disconnected
                | Some respTypeByte -> 
                    let respTypeInt = System.Convert.ToInt32(respTypeByte)
                    if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                        Eat5NoAlloc strm  
                        do! strm.AsyncWrite pongBytes
                        do! strm.FlushAsync() |> Async.AwaitTask
                    else
                        let respMsg = RespMsgParser.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm
//                        let! respMsg = AsyncRespMsgParser.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm
                        let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                        match choiceFredisCmd with 
                        | Choice1Of2 cmd    ->  let! resp = CmdProcChannel.MailBoxChannel cmd // to process the cmd on a single thread
//                                                let resp = FredisTypes.BulkString (FredisTypes.BulkStrContents.Contents pongBytes)
                                                do! RespStreamFuncs.AsyncSendResp strm resp
                                                do! strm.FlushAsync() |> Async.AwaitTask
                        | Choice2Of2 err    ->  do! RespStreamFuncs.AsyncSendError strm err
                                                do! strm.FlushAsync() |> Async.AwaitTask
        }




    Async.StartWithContinuations(
         asyncProcessClientRequests,
//         (fun _     -> printfn "ClientListener completed" ),
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

