open System.Net
open System.Net.Sockets

open FSharpx.Choice

open CmdCommon
open Utils



let host = """127.0.0.1"""
let port = 6379


let hashMap = HashMap()



let ClientListenerLoop (client:TcpClient) =
    printfn "new connection"

    let asyncProcessClientRequests =
        //let mutable (loopAgain:bool) = true
        let loopAgain = ref true
        async{
            use client = client // without this Dispose would not be called on client
            use ns = client.GetStream() 
            while (client.Connected && !loopAgain) do
                let! optRespTypeByte = ns.AsyncReadByte2()  // reading from the socket is synchronous after this point, until current redis msg is processed

                match optRespTypeByte with
                | None -> loopAgain := false
                | Some respTypeByte -> 
                    let respTypeInt = System.Convert.ToInt32(respTypeByte)
                    
                    let procResultBytes = choose{
                        let!    respMsg = RespMsgProcessor.LoadRESPMsgOuterChoice respTypeInt ns
                        let!    cmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                        return FredisCmdProcessor.Execute hashMap cmd
                        }

                    let replyBytes = 
                            match procResultBytes with 
                            | Choice1Of2 procCompleteBytes  -> procCompleteBytes
                            | Choice2Of2 errBytes           -> errBytes
                    
                    do! (ns.AsyncWrite replyBytes)
        }

    Async.StartWithContinuations(
         asyncProcessClientRequests,
         (fun _     -> printfn "ClientListener completed" ),
         (fun ex    -> printfn "ClientListener failed: %s" ex.Message),
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
         (fun _     -> printfn "ConnectionListener completed"),
         (fun ex    -> printfn "ConnectionListener failed: %s" ex.Message),
         (fun ct    -> printfn "ConnectionListener cancelled: %A" ct)
    )

let ipAddr = IPAddress.Parse(host)
let listener = TcpListener( ipAddr, port) 
listener.Start()
ConnectionListenerLoop listener
printfn "fredis startup complete\nawaiting incoming connection requests"
System.Console.ReadKey() |> ignore
//asyncConnectionListener.Cancel //#### shutdown all the clients, how? cancellationToken?
listener.Stop()


