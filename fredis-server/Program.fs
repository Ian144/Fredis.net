open System.Net
open System.Net.Sockets

open FSharpx.Choice

open CmdCommon
open Utils



//let host = """127.0.0.1"""
let host = """0.0.0.0"""
let port = 6379


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

//    client.ReceiveBufferSize <- 16 * 1024
//    client.NoDelay <- false
//    client.ReceiveTimeout <- 10000
    client.SendBufferSize <- 128 * 1024
    
    printfn "new connection"

//    let asyncProcessClientRequestsSimple =
//        //let mutable (loopAgain:bool) = true
//        let loopAgain = ref true
//
//        let totalBytesRead = ref 0
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
//                                // instead of 100 could have a number representing 512mb/receive buffer size
//                                let buffers = Array.create<byte[]> 100000 [||]
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
//                do! (ns.AsyncWrite okBytes)
//        }



    let asyncProcessClientRequests =
        //let mutable (loopAgain:bool) = true
        let loopAgain = ref true
        async{
            use client = client // without this Dispose would not be called on client
            use ns = client.GetStream() 
            while (client.Connected && !loopAgain) do
                let! optRespTypeByte = ns.AsyncReadByte2()  // reading from the socket is synchronous after this point, until current redis msg is processed

                match optRespTypeByte with
                | None ->   loopAgain := false
                | Some respTypeByte -> 
                            let respTypeInt = System.Convert.ToInt32(respTypeByte)
                            let choiceResp = choose{
                                    let!    respMsg = RespMsgProcessor.LoadRESPMsgOuterChoice client.ReceiveBufferSize respTypeInt ns 
                                    let!    cmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                                    return FredisCmdProcessor.Execute hashMap cmd 
                                }

                            let resp = 
                                    match choiceResp with 
                                    | Choice1Of2 resp  -> resp
                                    | Choice2Of2 _     -> FredisTypes.Resp.Error CmdCommon.errorBytes
                    
                            do! Utils.AsyncSendResp ns resp
        }

    Async.StartWithContinuations(
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



