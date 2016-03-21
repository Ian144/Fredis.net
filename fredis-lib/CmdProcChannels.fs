
[<RequireQualifiedAccess>]
module CmdProcChannel


open FredisTypes


let hashMap = CmdCommon.HashMap()




// not threadsafe, used for experiments only
let DirectChannel cmd =  async{ return FredisCmdProcessor.Execute hashMap cmd } 


type MboxMessage = FredisCmd * AsyncReplyChannel<Resp>


let private mbox =
    MailboxProcessor<MboxMessage>.Start( fun inbox ->
        let rec msgLoop () = async {
            let! cmd, replyChannel = inbox.Receive()
            let respReply = FredisCmdProcessor.Execute hashMap cmd 
            replyChannel.Reply respReply
            return! msgLoop () } 
        msgLoop () )



let MailBoxChannel (cmd:FredisCmd) =  mbox.PostAndAsyncReply (fun replyChannel -> cmd, replyChannel)


