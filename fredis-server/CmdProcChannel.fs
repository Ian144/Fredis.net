
[<RequireQualifiedAccess>]
module CmdProcChannel


open FredisTypes



let hashMap = CmdCommon.HashMap()


let DirectChannel (strm:System.IO.Stream) (cmd:FredisCmd) = 
    let respReply = FredisCmdProcessor.Execute hashMap cmd 
    StreamFuncs.AsyncSendResp strm respReply



let private mbox =
    MailboxProcessor.Start( fun inbox ->
        let rec msgLoop () =
            async { let! (strm:System.IO.Stream, cmd) = inbox.Receive()
                    let respReply = FredisCmdProcessor.Execute hashMap cmd 
                    do! StreamFuncs.AsyncSendResp strm respReply
                    return! msgLoop () } 
        msgLoop () )


let MailBoxChannel (strm:System.IO.Stream) (cmd:FredisCmd) = 
    mbox.Post (strm, cmd)
    async {return ()} // this async is here to maintain type signature compatibility with DirectChannel


let MailBoxChannel2 (strm:System.IO.Stream) (cmd:FredisCmd) = 
    async{
        mbox.Post (strm, cmd)
    } 