﻿open System.Net
open System.Net.Sockets

//open FSharpx.Choice

open CmdCommon
open Utils



//let host = """127.0.0.1"""
let host = """0.0.0.0"""
let port = 6379

// for responding to 'raw' non-RESP pings
[<Literal>]
let PingL = 80          // P - redis-benchmark just sends PING\r\n, i.e. a raw string, not RESP as i understood it
let pongBytes  = Utils.StrToBytes "+PONG\r\n"


let hashMap = HashMap()



let HandleSocketError (name:string) (ex:System.Exception) = 
    let rec HandleExInner (ex:System.Exception) = 
        let msg = ex.Message
        let optInnerEx = FSharpx.FSharpOption.ToFSharpOption ex.InnerException    

        match optInnerEx with
        | None  ->          ""
        | Some innerEx ->   let innerMsg = HandleExInner innerEx
                            sprintf "%s | %s" msg innerMsg
    
    let msg = HandleExInner ex
    printfn "%s --> %s" name msg

let ClientError ex =  HandleSocketError "client error" ex
let ConnectionListenerError ex = HandleSocketError "connection listener error" ex


let ClientListenerLoop (client:TcpClient) =

    //#### todo set from config
    client.NoDelay <- true // disable Nagles algorithm, don't want small messages to be held back for buffering
    client.ReceiveBufferSize <- 64 * 1024
    client.SendBufferSize <- 64 * 1024
    
    printfn "new connection"


//    let asyncProcessClientRequestsSimple =
//        //let mutable (loopAgain:bool) = true
//        let loopAgain = ref true
//
//        let pongBytes  = Utils.StrToBytes "+PONG\r\n"
//        let totalBytesRead = ref 0
//        let buffers = Array.create<byte[]> 100000 [||]
//
//        async{
//            use client = client // without this Dispose would not be called on client
//            use ns = client.GetStream() 
//            while (client.Connected && !loopAgain) do
//                let! optRespTypeByte = ns.AsyncReadByte2()  // reading from the socket is synchronous after this point, until current redis msg is processed
//                printfn "handle new command"
//                match optRespTypeByte with
//                | None              -> loopAgain := false
//                | Some firstByte    ->
//                                let ctr = ref 0
//                                while client.Available > 0 do
//                                    let availToRead = client.Available
//                                    let buffer = Array.zeroCreate<byte> availToRead
//                                    let numBytesRead = ns.Read(buffer,0,availToRead) 
//                                    let idx:int = !ctr
//                                    buffers.[idx] <- buffer    
//                                    totalBytesRead := !totalBytesRead + numBytesRead 
//                                    ctr := !ctr + 1
//                                    printfn "numBytesRead: %d" numBytesRead
//                                let allBytes:byte[] =  buffers |> Array.collect id
//                                let ss = Utils.BytesToStr allBytes
//                                let firstChar = System.Convert.ToChar firstByte
//                                printfn "read:\n%c%s" firstChar  ss
//                                printfn "total numBytesRead: %d" !totalBytesRead
//                do! (ns.AsyncWrite pongBytes)
//        }


    let asyncProcessClientRequests =
        //let mutable (loopAgain:bool) = true
        let loopAgain = ref true
        async{
            use client = client // without this Dispose would not be called on client
            use strm = client.GetStream() 
            while (client.Connected && !loopAgain) do

                // reading from the socket is mostly synchronous after this point, until current redis msg is processed
                let! optRespTypeByte = strm.AsyncReadByte2()  
                match optRespTypeByte with
                | None ->   loopAgain := false  // client disconnected
                | Some respTypeByte -> 
                            let respTypeInt = System.Convert.ToInt32(respTypeByte)
                            if respTypeInt = PingL then 
//                                Eat5BytesX bs strm
                                Eat5NoArray strm  // redis-cli and redis-benchmark send pings (PING_INLINE) as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                                //Eat5Bytes strm  // redis-cli and redis-benchmark send pings (PING_INLINE) as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                                do! strm.AsyncWrite pongBytes
                            else
                                let respMsg = RespMsgProcessor.LoadRESPMsg client.ReceiveBufferSize respTypeInt strm // invalid RESP will cause an exception here which will kill the client connection
                                let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                                match choiceFredisCmd with 
                                | Choice1Of2 cmd    ->  let respReply = FredisCmdProcessor.Execute hashMap cmd 
                                                        do! Utils.AsyncSendResp strm respReply        
                                | Choice2Of2 err    ->  do! strm.AsyncWrite err // the err strings are already in RESP format
        }


    Async.StartWithContinuations(
//         asyncProcessClientRequestsSimple,
         asyncProcessClientRequests,
         (fun _     -> printfn "ClientListener completed" ),
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
        (fun ex -> ConnectionListenerError ex),
        (fun ct -> printfn "ConnectionListener cancelled: %A" ct)
    )


let ipAddr = IPAddress.Parse(host)
let listener = TcpListener( ipAddr, port) 
listener.Start ()
ConnectionListenerLoop listener
printfn "fredis startup complete\nawaiting incoming connection requests"
System.Console.ReadKey() |> ignore
do Async.CancelDefaultToken()
printfn "cancelling asyncs"
System.Console.ReadKey() |> ignore
listener.Stop()
printfn "stopped"



