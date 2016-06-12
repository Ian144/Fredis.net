open System.Net
open System.Net.Sockets



let host = """0.0.0.0"""
let port = 6379



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
        MsgLoops.ConnectionListenerLoop bufSize listener
        printfn "fredis.net startup complete\nawaiting incoming connection requests\npress 'X' to exit"
        WaitForExitCmd ()
        do Async.CancelDefaultToken()
        printfn "cancelling asyncs"
        listener.Stop()
        printfn "stopped"
        0 // return an integer exit code
    | Choice2Of2 msg -> printf "%s" msg
                        1 // non-zero exit code

