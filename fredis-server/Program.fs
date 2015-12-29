open System.Net
open System.Net.Sockets

open Utils
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
    printfn "%s --> %s" name msg

let ClientError ex =  HandleSocketError "client error" ex
let ConnectionListenerError ex = HandleSocketError "connection listener error" ex




//type BufferSegment =
//  { Buffer : System.ArraySegment<byte>
//    Offset : int
//    Length : int }
//
//
//[<NoComparison>]
//type Connection =  { 
//    Client      : TcpClient
//    Segments    : BufferSegment list }
//    with member this.Async.ReadByte   



let ClientListenerLoop (client:TcpClient) =

    //#### todo set from config
    client.NoDelay <- true // disable Nagles algorithm, don't want small messages to be held back for buffering
    client.ReceiveBufferSize    <- 8 * 1024
    client.SendBufferSize       <- 8 * 1024
    
//    printfn "new connection"
//    let buf = Array.zeroCreate<byte>(256)
    let buf = Array.zeroCreate 1 

    let asyncProcessClientRequests =
        //let mutable (loopAgain:bool) = true // enable with F#4.0
        let loopAgain = ref true
        async{
            use client = client // without this Dispose would not be called on client
            use strm = client.GetStream() 
            while (client.Connected && !loopAgain) do

                // reading from the socket is mostly synchronous after this point, until current redis msg is processed
                let! optRespTypeByte = strm.AsyncReadByte3 buf
                match optRespTypeByte with
                | None              ->
                    loopAgain := false  // client disconnected
                | Some respTypeByte -> 
                    let respTypeInt = System.Convert.ToInt32(respTypeByte)
                    if respTypeInt = PingL then 
                        Eat5NoArray strm  // redis-cli and redis-benchmark send pings (PING_INLINE) as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                        do! strm.AsyncWrite pongBytes
                    else
//                        let respMsg = RespMsgProcessor.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm //#### receiving invalid RESP will cause an exception here which will kill the client connection
                        let! respMsg = AsyncRespMsgProcessor.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm
//                        printfn "received: %A" respMsg
                        let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                        match choiceFredisCmd with 
                        | Choice1Of2 cmd    ->  do CmdProcChannel.DisruptorChannel (strm, cmd)
                        | Choice2Of2 err    ->  do! RespStreamFuncs.AsyncSendError strm err
                        //| Choice2Of2 err    ->  do! strm.AsyncWrite err // err strings are in RESP format
        }




    Async.StartWithContinuations(
         asyncProcessClientRequests,
//         (fun _     -> printfn "ClientListener completed" ),
         ignore,
         ClientError,
         (fun ct    -> printfn "ClientListener cancelled: %A" ct)
    )

let ConnectionListenerLoop (listener:TcpListener) =
    let asyncConnectionListener =
        async {
            while true do
                let acceptClientTask = listener.AcceptTcpClientAsync ()
                let! client = Async.AwaitTask acceptClientTask
                do ClientListenerLoop client
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

//let rec WaitForExitCmd () = 
//    match System.Console.ReadKey().KeyChar with
//    | 'X'   -> ()
//    | _     -> WaitForExitCmd ()


let ipAddr = IPAddress.Parse(host)
let listener = TcpListener( ipAddr, port) 
listener.Start ()
ConnectionListenerLoop listener
printfn "fredis startup complete\nawaiting incoming connection requests\npress 'X' to exit"
WaitForExitCmd ()
do Async.CancelDefaultToken()
printfn "cancelling asyncs"
listener.Stop()
printfn "stopped"



