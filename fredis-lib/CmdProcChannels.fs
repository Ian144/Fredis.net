
[<RequireQualifiedAccess>]
module CmdProcChannel


open FredisTypes



let hashMap = CmdCommon.HashMap()


// this struct has public, mutable members as its used to populate the ringbuffer
[<NoComparison; NoEquality>]
type CmdProcChannelMsg =
    struct
        val mutable Strm: System.IO.Stream
        val mutable Cmd: FredisCmd
//        new(strmIn: System.IO.Stream, cmdIn: FredisCmd) = { Strm = strmIn; Cmd = cmdIn }
    end


//let DirectChannel (strm:System.IO.Stream, cmd:FredisCmd) = 
//    let respReply = FredisCmdProcessor.Execute hashMap cmd 
//    StreamFuncs.AsyncSendResp strm respReply

let private mbox =
    MailboxProcessor.Start( fun inbox ->
        let rec msgLoop () =
            async { 
                    let! (strm:System.IO.Stream, cmd) = inbox.Receive()
                    try
                        let respReply = FredisCmdProcessor.Execute hashMap cmd 
                        do! RespStreamFuncs.AsyncSendResp strm respReply
                    with
                    | ex -> 
                        printfn "mailbox processor exception thrown: %s - %A" ex.Message (ex.GetType())
                        //do! strm.AsyncWrite (RespUtils.errorBytes, 0, RespUtils.errorBytes.Length) // strm has been disposed
                    
                    return! msgLoop () } 
        msgLoop () )


let MailBoxChannel (strm:System.IO.Stream, cmd:FredisCmd) = 
    mbox.Post (strm, cmd)



//open Disruptor
//open System.Threading
//
//let private bufSize = 8192
//let private lBufSize = int64(bufSize)
//let private nSpin = 1024
//
//// used for fast remainder calc: x % 2^n == x & (2^n - 1) when x, bufSize in this case, is a power of 2.
//// sequences are int64's, array indices are ints.
//// indexMask needs to be 'anded' with an int64 sequence number, then the result converted to an int to get a ringbuffer index
//let private indexMask = lBufSize - 1L
//
//
//type private Sequence = Padded.Sequence
//
//let private seqWrite = Sequence initialSeqVal
//let private seqWriteC = Sequence initialSeqVal
//let private seqRead  = Sequence initialSeqVal
//let private ringBuffer = Array.zeroCreate<CmdProcChannelMsg>(bufSize) // elements initialised with the struct default ctor>(bufSize) // elements initialised with the struct default ctor
//
//
//let pongBytes  = Utils.StrToBytes "+PONG\r\n"
//
//
//let private DisruptorConsumerFunc () = 
//    printfn "starting new consumer"
//
//    let mutable ctr = 0L
//    while true do
//        let freeUpTo = ConsumerWait (seqRead._value, seqWriteC) // spinWhileEqual reading seqWriteC accross caches
//        while ctr <= freeUpTo do
//            let indx = int(ctr &&& indexMask) // convert the int64 sequence number to an int ringBuffer index
//            let msg = ringBuffer.[indx]
//            seqRead._value <- ctr   // publish position 
//            ctr <- ctr + 1L
//
//            // (un)commenting can show how relative cost of FredisCmdProcessor.Execute vs StreamFuncs.AsyncSendResp vs RESP decoding
//            let respReply = FredisCmdProcessor.Execute hashMap msg.Cmd
////            printfn "replying with: %A" respReply
//            let asyncSend = RespStreamFuncs.AsyncSendResp msg.Strm respReply 
////            let asyncSend = msg.Strm.AsyncWrite pongBytes 
//
//            Async.StartWithContinuations(
//                asyncSend,
//                ignore,
//                (fun ex -> printfn "DisruptorConsumerFunc send exception: %s" ex.Message),
//                (fun ct -> printfn "DisruptorConsumerFunc send cancelled: %A" ct)
//            )
//
//            
//
//let private consumerTask = Tasks.Task.Factory.StartNew( 
//                                    DisruptorConsumerFunc, 
//                                    CancellationToken.None, 
//                                    Tasks.TaskCreationOptions.None, 
//                                    Tasks.TaskScheduler.Default )
//
//
//let DisruptorChannel (strm:System.IO.Stream, cmd:FredisCmd) = 
//    let writeSeqVal = ProducerWaitCAS (lBufSize, seqWrite, seqRead)
//    let indx = int(writeSeqVal &&& indexMask)
//    ringBuffer.[indx].Strm <- strm
//    ringBuffer.[indx].Cmd <- cmd         // it is safe to write here, as ProducerWaitCAS has allocated this slot for this producer
//    let prevSeqValToWaitOn = writeSeqVal - 1L       // wait until the previous slot has been published, probably by some other producer thread
//    
//    
//    let mutable seqWriteCVal = Thread.VolatileRead ( & seqWriteC._value )
//    while prevSeqValToWaitOn <> seqWriteC._value do  // ensure the earlier slots have been published before publishing this one
//        seqWriteCVal <- Thread.VolatileRead ( & seqWriteC._value )
//    seqWriteC._value <- writeSeqVal                  // publish, cant be written to by multiple threads because they are spin waiting on a different value of prevSeqValToWaitOn
//
//
