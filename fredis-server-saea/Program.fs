open System
open System.Net
open System.Net.Sockets
open System.IO
open System.Collections.Concurrent
open System.Threading.Tasks

open RespStreamFuncs



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



let OnClientIOCompleted (saea:SocketAsyncEventArgs) =
    match saea.LastOperation with
    | SocketAsyncOperation.Receive  -> ()
    | SocketAsyncOperation.Send     -> ()
    | _                             -> failwith "expected SocketAsyncOperation"


let maxNumConnections = 1024
let saeaBufSize = 1024 * 32
let saeaSharedBuffer = Array.zeroCreate<byte> (maxNumConnections * saeaBufSize)
let saeaPool = new ConcurrentStack<SocketAsyncEventArgs>()

for ctr = 0 to (maxNumConnections - 1) do
    let saea = new SocketAsyncEventArgs()
    let offset = ctr*saeaBufSize
    saea.SetBuffer(saeaSharedBuffer, offset, saeaBufSize)
    saea.Completed.Subscribe  OnClientIOCompleted  |> ignore
    saeaPool.Push(saea)


type UserToken = {
    mutable Socket: Socket
    mutable Tcs: TaskCompletionSource<byte []>
    mutable ClientBuf: byte[]
    mutable ClientBufPos: int
    mutable SaeaBufStart: int
    mutable SaeaBufEnd: int
    }

    
type UserToken2<'t> = {
    mutable Socket: Socket
    mutable Tcs: TaskCompletionSource<'t>
    mutable ClientBuf: byte[]
    mutable ClientBufPos: int
    mutable SaeaBufStart: int
    mutable SaeaBufEnd: int
    }


let rec ProcessReceive (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    let bytesTransferred = saea.BytesTransferred
    let bytesRequired = ut.ClientBuf.Length - ut.ClientBufPos

    match saea.SocketError, bytesTransferred, bytesRequired with
    | SocketError.Success, tran, req when req = tran ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset, ut.ClientBuf, ut.ClientBufPos, req)
            ut.ClientBufPos <- ut.ClientBuf.Length
            ut.SaeaBufStart <- bytesRequired
            ut.SaeaBufEnd   <- bytesTransferred

    | SocketError.Success, tran, req when req < tran ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset, ut.ClientBuf, ut.ClientBufPos, req)
            ut.ClientBufPos <- ut.ClientBuf.Length
            ut.SaeaBufStart <- saeaBufSize
            ut.SaeaBufEnd   <- saeaBufSize

    | SocketError.Success, tran, req when req > tran ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset, ut.ClientBuf, ut.ClientBufPos, bytesTransferred)
            ut.ClientBufPos <- ut.ClientBufPos + bytesTransferred
            ut.SaeaBufStart <- saeaBufSize
            ut.SaeaBufEnd   <- saeaBufSize
            let ioPending = ut.Socket.ReceiveAsync saea
            if not ioPending then
                ProcessReceive(saea)

    | SocketError.Success, 0, _ -> 
            ut.Tcs.SetCanceled()    // client has disconnected

    | err, _, _ ->
            let msg = sprintf "receive socket error: %O" err
            let ex = new Exception(msg)
            ut.Tcs.SetException(ex)




// requires a buffer to be supplied, enabling pre-allocated buffers to be reused
let AsyncRead (saea:SocketAsyncEventArgs) (dest:byte []) : Async<byte[]> =
    let ut = saea.UserToken :?> UserToken
    ut.ClientBuf <- dest
    ut.ClientBufPos <- 0   // AsyncRead will always read into the client Buffer starting at index zero

    let availableBytes = ut.SaeaBufEnd - ut.SaeaBufStart // some bytes may have been read-in already, by a previous read operation

    match availableBytes with
    | _ when availableBytes >= dest.Length ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset + ut.SaeaBufStart, dest, 0, dest.Length) // able to satisfy the entire read request from bytes already available
            ut.SaeaBufStart <- ut.SaeaBufStart + dest.Length // mark the new begining of the unread bytes
            Task.FromResult(dest) |> Async.AwaitTask

    | _ when availableBytes > 0 ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset + ut.SaeaBufStart, dest, 0, availableBytes)
            ut.ClientBufPos <- availableBytes // availableBytes have been written into the client array (aka 'dest'), so the first unread index is 'availableBytes' because dest is zero based
            let tcs = new TaskCompletionSource<byte[]>()
            ut.Tcs <- tcs
            ut.SaeaBufStart <- saeaBufSize
            ut.SaeaBufEnd   <- saeaBufSize
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessReceive(saea)
            tcs.Task  |> Async.AwaitTask

    | _ ->
            let tcs = new TaskCompletionSource<byte[]>()
            ut.Tcs <- tcs
            ut.SaeaBufStart <- saeaBufSize
            ut.SaeaBufEnd   <- saeaBufSize
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessReceive(saea)
            tcs.Task |> Async.AwaitTask


// does not require a buffer to be supplied
let AsyncRead2 (saea:SocketAsyncEventArgs) (count:int) : Async<byte[]> =
    let dest = Array.zeroCreate count
    AsyncRead saea dest



let rec ProcessSend (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    let bytesTransferred = saea.BytesTransferred
    ut.ClientBufPos <- ut.ClientBufPos + bytesTransferred

    assert (ut.ClientBufPos <= ut.ClientBuf.Length)

    match saea.SocketError, ut.ClientBuf.Length - ut.ClientBufPos with
    | SocketError.Success, 0 -> 
            Buffer.BlockCopy(ut.ClientBuf, ut.ClientBufPos, saea.Buffer, 0, bytesTransferred)
            ut.Tcs.SetResult(null) // todo: ut.Tcs.SetResult(null) - how is a non-generic Task signalled as being complete

    | SocketError.Success, lenRemaining    ->
            Buffer.BlockCopy(ut.ClientBuf, ut.ClientBufPos, saea.Buffer, 0, bytesTransferred)
            ut.Tcs.SetResult(null) // todo: ut.Tcs.SetResult(null) - how is a non-generic Task signalled as being complete
            let lenToSend = 
                    if lenRemaining > saeaBufSize
                    then saeaBufSize
                    else lenRemaining
            saea.SetBuffer(0, lenToSend);
            let ioPending = ut.Socket.SendAsync(saea)
            if not ioPending then
                ProcessSend(saea)

    | err   ->
            let msg = sprintf "send socket error: %O" err
            let ex = new Exception(msg)
            ut.Tcs.SetException(ex)





let AsyncWrite (saea:SocketAsyncEventArgs) (bs:byte[]) : Async<unit> =
    let ut = saea.UserToken :?> UserToken
    ut.ClientBuf <- bs
    ut.ClientBufPos <- 0
    let tcs = new TaskCompletionSource<byte[]>()
    ut.Tcs <- tcs

    match bs.Length <= saeaBufSize with
    | true  ->
            Buffer.BlockCopy(bs, 0, saea.Buffer, 0, bs.Length)
            saea.SetBuffer(0, bs.Length)
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessSend(saea)
            tcs.Task :> Task |> Async.AwaitTask   //todo: converting a Task<byte[]> to a non-generic Task, this requires fixing
    | false ->
            Buffer.BlockCopy(bs, 0, saea.Buffer, 0, saeaBufSize)
            ut.ClientBufPos <- saeaBufSize
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessSend(saea)
            tcs.Task :> Task  |> Async.AwaitTask  //todo: converting a Task<byte[]> to a non-generic Task, this requires fixing




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

//        client.ReceiveBufferSize    <- bufSize
//        client.SendBufferSize       <- bufSize

        let buf1 = Array.zeroCreate 1
        let buf5 = Array.zeroCreate 5   // used to eat PONG msgs

        let asyncProcessClientRequests =
            let mutable loopAgain = true
            async{
                // msdn: "It is assumed that you will almost always be doing a series of reads or writes, but rarely alternate between the two of them"
                // Fredis does alternate between reads and writes, but tests have shown that BufferedStream still gives a perf boost without errors
                // BufferedStream will deadlock if there are simultaneous async reads and writes in progress, due to an internal semaphore. But works if this is not the case.
                // The F# async workflow sequences async reads and writes so none are simultaneous.
                use strm = new System.IO.BufferedStream ( netStrm, bufSize )
                while (client.Connected ) do
                    let! _ = AsyncRead saea buf1  // todo: does " let! _ = AsyncRead" handle client disconnections, where the Task inside the Async has been cancelled
                    let respTypeInt = System.Convert.ToInt32(buf1.[0])
                    if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                        // todo: could manually adjust the saea userToken to eat 5 chars
                        let! _ = AsyncRead saea buf5        // todo: let! _ is ugly, fix
                        do! AsyncWrite saea pongBytes 
                        ()
                    else
//                        let respMsg = RespMsgProcessor.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm
                        let! respMsg = AsyncRespMsgProcessor.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm
                        let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                        match choiceFredisCmd with
                        | Choice1Of2 cmd    ->  let! resp = CmdProcChannel.MailBoxChannel cmd // to process the cmd on a single thread
                                                do! RespStreamFuncs.AsyncSendResp strm resp
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

