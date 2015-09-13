open System.Net
open System.Net.Sockets

open Utils
open StreamFuncs


//let host = """127.0.0.1"""
let host = """0.0.0.0"""
let port = 6379

// for responding to 'raw' non-RESP pings
[<Literal>]
let PingL = 80  // P - redis-benchmark PING_INLINE just sends PING\r\n, not encoded as RESP
let pongBytes  = Utils.StrToBytes "+PONG\r\n"



let HandleSocketError (name:string) (ex:System.Exception) = 
    let rec HandleExInner (ex:System.Exception) = 
        let msg = ex.Message
        let optInnerEx = FSharpx.FSharpOption.ToFSharpOption ex.InnerException    

        match optInnerEx with
        | None  ->          msg
        | Some innerEx ->   let innerMsg = HandleExInner innerEx
                            sprintf "%s | %s" msg innerMsg
    
    let msg = HandleExInner ex
    printfn "%s --> %s" name msg

let ClientError ex =  HandleSocketError "client error" ex
let ConnectionListenerError ex = HandleSocketError "connection listener error" ex


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
                | None              ->   loopAgain := false  // client disconnected
                | Some respTypeByte -> 
                            let respTypeInt = System.Convert.ToInt32(respTypeByte)
                            if respTypeInt = PingL then 
                                Eat5NoArray strm  // redis-cli and redis-benchmark send pings (PING_INLINE) as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                                do! strm.AsyncWrite pongBytes
                            else
                                let respMsg = RespMsgProcessor.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm //#### receiving invalid RESP will cause an exception here which will kill the client connection
                                let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                                match choiceFredisCmd with 
                                | Choice1Of2 cmd    ->  do! CmdProcChannel.DisruptorChannel (strm, cmd)
                                | Choice2Of2 err    ->  do! strm.AsyncWrite err // err strings are in RESP format
        }



    Async.Start asyncProcessClientRequests

//    Async.StartWithContinuations(
//         asyncProcessClientRequests,
//         (fun _     -> printfn "ClientListener completed" ),
//         ClientError,
//         (fun ct    -> printfn "ClientListener cancelled: %A" ct)
//    )

let ConnectionListenerLoop (listener:TcpListener) =
    let asyncConnectionListener =
        async {
            while true do
                let acceptClientTask = listener.AcceptTcpClientAsync ()
                let! client = Async.AwaitTask acceptClientTask
                do ClientListenerLoop client
        }  

    Async.Start asyncConnectionListener   
//    Async.StartWithContinuations(
//        asyncConnectionListener,
//        (fun _  -> printfn "ConnectionListener completed"),
//        (fun ex -> ConnectionListenerError ex),
//        (fun ct -> printfn "ConnectionListener cancelled: %A" ct)
//    )


let ipAddr = IPAddress.Parse(host)
let listener = TcpListener( ipAddr, port) 
listener.Start ()
ConnectionListenerLoop listener
printfn "fredis startup complete\nawaiting incoming connection requests\npress any key to exit"
System.Console.ReadKey() |> ignore
printfn ""
do Async.CancelDefaultToken()
printfn "cancelling asyncs"
listener.Stop()
printfn "stopped"



