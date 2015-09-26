
[<RequireQualifiedAccess>]
module CmdProcChannel


open FredisTypes



let hashMap = CmdCommon.HashMap()





let DirectChannel (strm:System.IO.Stream, cmd:FredisCmd) = 
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


let MailBoxChannel (strm:System.IO.Stream, cmd:FredisCmd) = 
    mbox.Post (strm, cmd)
    async {return ()} // this async is here to maintain type signature compatibility with DirectChannel


let MailBoxChannel2 (strm:System.IO.Stream, cmd:FredisCmd) = 
    async{
        mbox.Post (strm, cmd)
    } 


open Disruptor
open System.Threading

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
let private ringBuffer = Array.zeroCreate<System.IO.Stream * FredisCmd>(bufSize) // elements initialised with the struct default ctor



let pongBytes  = Utils.StrToBytes "+PONG\r\n"


let private DisruptorChannelConsumer () = 
    let mutable ctr = 0L
    while true do
        let freeUpTo = ConsumerWait (seqRead._value, seqWriteC)
        while ctr <= freeUpTo do
            let indx = int(ctr &&& indexMask) // convert the int64 sequence number to an int ringBuffer index
            let (strm, cmd) = ringBuffer.[indx]
            seqRead._value <- ctr   // publish position //#### confirm OK without Thread.VolatileWrite, x86-64 memory model is strong
            let respReply = FredisCmdProcessor.Execute hashMap cmd 
            let asyncSend = StreamFuncs.AsyncSendResp strm respReply 
//            if (ctr % 1000L) = 0L then
//                printfn "msg async reply: %d" ctr
            Async.Start asyncSend // f# asyncs are not started implicitly
            ctr <- ctr + 1L

            

//let private consumerTask = Tasks.Task.Factory.StartNew( 
//                                    DisruptorChannelConsumer, 
//                                    CancellationToken.None, 
//                                    Tasks.TaskCreationOptions.None, 
//                                    Tasks.TaskScheduler.Default )


let consumerThread = new Thread(DisruptorChannelConsumer)
consumerThread.Start()


let DisruptorChannel (evnt:System.IO.Stream * FredisCmd) = 
//    printfn "request"
    let writeSeqVal = ProducerWaitCAS (lBufSize, seqWrite, seqRead)   
    let indx = int(writeSeqVal &&& indexMask)
    ringBuffer.[indx] <- evnt                       // it is safe to write here, as ProducerWaitCAS has allocated this slot for this producer
    let prevSeqValToWaitOn = writeSeqVal - 1L       // wait until the previous slot has been published, probably by some other producer thread
    while prevSeqValToWaitOn <> seqWriteC._value do  // ensure the earlier slots have been published before publishing this one
//        Thread.SpinWait(nSpin)  
                () // busy wait
    seqWriteC._value <- writeSeqVal                  // publish, cant be written to by multiple threads because they are spin waiting on a different value of prevSeqValToWaitOn
    async {return ()} // this async is here to maintain type signature compatibility with DirectChannel




