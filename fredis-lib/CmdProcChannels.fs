
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


open Disruptor
open System.Threading


// this struct has public, mutable members as it is used to populate the ringbuffer
[<NoComparison; NoEquality>]
type CmdProcChannelMsg =
    struct
        val mutable Cmd: FredisCmd
        val mutable Tcs: System.Threading.Tasks.TaskCompletionSource<Resp>
    end


let private bufSize = 8192
let private lBufSize = int64(bufSize)
let private nSpin = 1024

// used for fast remainder calc: x % 2^n == x & (2^n - 1) when x, bufSize in this case, is a power of 2.
// sequences are int64's, array indices are ints.
// indexMask needs to be 'anded' with an int64 sequence number, then the result converted to an int to get a ringbuffer index
let private indexMask = lBufSize - 1L


type private Sequence = Padded.Sequence

let private seqWrite = Sequence initialSeqVal
let private seqWriteC = Sequence initialSeqVal
let private seqRead  = Sequence initialSeqVal
let private ringBuffer = Array.zeroCreate<CmdProcChannelMsg>(bufSize) // elements initialised with the struct default ctor>(bufSize) // elements initialised with the struct default ctor



let private DisruptorConsumerFunc () = 

    let mutable ctr = 0L
    while true do
        let freeUpTo = ConsumerWait (seqRead._value, seqWriteC) // spinWhileEqual reading seqWriteC across caches
        while ctr <= freeUpTo do
            let indx = int(ctr &&& indexMask) // convert the int64 sequence number to an int ringBuffer index
            let msg = ringBuffer.[indx]
            seqRead._value <- ctr   // publish position 
            ctr <- ctr + 1L
            let respReply = FredisCmdProcessor.Execute hashMap msg.Cmd
            msg.Tcs.SetResult(respReply)



            

//let private consumerTask = Tasks.Task.Factory.StartNew( 
//                                    DisruptorConsumerFunc, 
//                                    CancellationToken.None, 
//                                    Tasks.TaskCreationOptions.None, 
//                                    Tasks.TaskScheduler.Default )


let DisruptorChannel (cmd:FredisCmd) : Async<Resp> = 
    let tcs = System.Threading.Tasks.TaskCompletionSource<Resp>() // todo can TaskCompletionSources be reused, to avoid an alloc with every send
    let writeSeqVal = ProducerWaitCAS (lBufSize, seqWrite, seqRead)
    let indx = int(writeSeqVal &&& indexMask)
    ringBuffer.[indx].Cmd <- cmd         // it is safe to write here, as ProducerWaitCAS has allocated this slot for this producer
    ringBuffer.[indx].Tcs <- tcs
    let prevSeqValToWaitOn = writeSeqVal - 1L       // wait until the previous slot has been published, probably by some other producer thread
    
    
    let mutable seqWriteCVal = Thread.VolatileRead ( & seqWriteC._value )
    while prevSeqValToWaitOn <> seqWriteC._value do  // ensure the earlier slots have been published before publishing this one
        seqWriteCVal <- Thread.VolatileRead ( & seqWriteC._value )
    seqWriteC._value <- writeSeqVal                  // publish, cant be written to by multiple threads because they are spin waiting on a different value of prevSeqValToWaitOn

    Async.AwaitTask tcs.Task
