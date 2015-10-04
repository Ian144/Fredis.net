module TestfDisruptor


open Xunit
open FsCheck
open FsCheck.Xunit


open System.Threading
open System.Collections.Generic



type RingBufSize = 
    static member Ints() =
        Gen.choose(1, 16)
        |> Gen.map (fun pwr -> pown 2 pwr)
        |> Arb.fromGen


type NumThreads = 
    static member Ints() =
        Gen.choose(1, 128)
        |> Arb.fromGen


//[<Property( Arbitrary = [| typeof<RingBufSize>; typeof<NumThreads> |])>]
//let ``ReadInt64 basic`` (sz:int) (nt:int64) =
//    printfn "ringBufSize: %d - num threads: %d" sz nt
//    true


type private Sequence = Padded.Sequence



let private Producerfunc (numMsgs:int, ringBuffer:(int*int) array, seqRead:Sequence, seqWrite:Sequence, seqWriteC:Sequence, lBufSize:int64, indexMask:int64) = 

    let tid = Thread.CurrentThread.ManagedThreadId

    for ctr = 0 to (numMsgs - 1) do
        let writeSeqVal = Disruptor.ProducerWaitCAS (lBufSize, seqWrite, seqRead)  
        let indx = int(writeSeqVal &&& indexMask)
        ringBuffer.[indx] <- (tid, ctr)
        let prevSeqValToWaitOn = writeSeqVal - 1L       // wait until the previous slot has been published, probably by some other producer thread
        while prevSeqValToWaitOn <> seqWriteC._value do  // ensure the earlier slots have been published before publishing this one
            Thread.SpinWait(0)
        seqWriteC._value <- writeSeqVal                  // publish, cant be written too by multiple threads because they are spin waiting on a different value of prevSeqValToWaitOn
    


let private ConsumerFunc (totalNumMsgs:int, msgsReceived:System.Collections.Generic.List<int*int>, ringBuffer:(int*int) array, seqRead:Sequence, seqWriteC:Sequence, lBufSize:int64, indexMask:int64) = 
    let mutable ctr = 0L
    let total = int64 (totalNumMsgs)
    while ctr < total do
        let freeUpTo = Disruptor.ConsumerWait (seqRead._value, seqWriteC)
        while ctr <= freeUpTo do
            let indx = int(ctr &&& indexMask) // convert the int64 sequence number to an int ringBuffer index
            let msgReceived = ringBuffer.[indx]
            seqRead._value <- ctr   // publish position 
            msgsReceived.Add msgReceived
            ctr <- ctr + 1L
                    

// test that all messages are received, and that the ordering of messages from individual producers is preserved
let TestDisruptor (bufSize:int) (numProducers:int) (numMsgsPerProducer:int)  = 

    let lBufSize = int64(bufSize)
    let nSpin = 1024    
    let indexMask = lBufSize - 1L
    let seqWrite =      Sequence Disruptor.initialSeqVal
    let seqWriteC =     Sequence Disruptor.initialSeqVal
    let seqRead  =      Sequence Disruptor.initialSeqVal
    let ringBuffer =    Array.zeroCreate<int*int>(bufSize)
    let totalNumMsgs = numMsgsPerProducer*numProducers

    // create consumer task
    let msgsReceived = System.Collections.Generic.List<int*int>(totalNumMsgs)
    let consumerAction () = ConsumerFunc (totalNumMsgs, msgsReceived, ringBuffer, seqRead, seqWriteC, lBufSize, indexMask )
    let consumerTask = Tasks.Task.Factory.StartNew consumerAction 
     
    // create producer tasks
    let prodParams = (numMsgsPerProducer, ringBuffer, seqRead, seqWrite, seqWriteC, lBufSize, indexMask )

    printfn "producers starting"

    // cant use tasks for producing, the test requires a separate thread for each sender
    let producerThreads =
                        [   for _ in 0 .. (numProducers - 1)  do
                            let prodAction () = Producerfunc prodParams
                            let thrd = System.Threading.Thread( prodAction )
                            thrd.Start()
                            yield thrd ]
    
    consumerTask.Wait()

    producerThreads |> List.iter (fun thrd -> thrd.Join() )

    printfn "all tasks/threads finished"

    let allMsgsReceived = msgsReceived.Count = totalNumMsgs

//     get rid of the List.ofSeq when using F# 4.O, which has List.groupBy
//     group msgs received by sending threadID into by tid subsequences
//     then check all subsequences are
//          the same length,
//          sorted - indicating msg ordering is preserved

    let xxs = msgsReceived 
                |> Seq.groupBy (fun (tid,_) -> tid) // group by threadid
                |> Seq.map (fun (_,xs) -> xs |> Seq.map snd |> List.ofSeq) // strip the threadID, both the groupby key and the first tuple element
                |> Seq.toList

    let expected = [0..(numMsgsPerProducer-1)]

    let allSortedAscending = xxs |> List.forall (fun xs -> xs = expected)

    let badSeqs = xxs |> Seq.filter (fun xs -> xs <> expected) |> List.ofSeq


    let ok = allMsgsReceived && allSortedAscending
    ok





[<Fact>]
let ``Test fDisruptor`` () =

    let ringBufSize = 1024 * 1024
    let numProducers = 8
    let numMsgsPerProducer =  1024 * 1024 / numProducers

    <@TestDisruptor ringBufSize numProducers numMsgsPerProducer@>



