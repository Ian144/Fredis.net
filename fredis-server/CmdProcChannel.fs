
[<RequireQualifiedAccess>]
module CmdProcChannel


open FredisTypes



let hashMap = CmdCommon.HashMap()


let DirectChannel (strm:System.IO.Stream) (cmd:FredisCmd) = 
    let respReply = FredisCmdProcessor.Execute hashMap cmd 
    StreamFuncs.AsyncSendResp strm respReply


//#### call mbox.Dispose on shutdown
//#### confirm the default async cancellation will shutdown the mailbox listening loop

let mbox =
    MailboxProcessor.Start( fun inbox ->
        let rec loop () =
            async { do printfn "waiting"
                    let! msg = inbox.Receive()
                    do printfn "msg received"
                    do! match msg with 
                        | strm,cmd ->   let respReply = FredisCmdProcessor.Execute hashMap cmd 
                                        StreamFuncs.AsyncSendResp strm respReply
                    return! loop() }
        loop () )


let MailBoxChannel (strm:System.IO.Stream) (cmd:FredisCmd) = 
    mbox.Post (strm,cmd)
    async {return ()} // this async is here to maintain type signature compatibility with DirectChannel