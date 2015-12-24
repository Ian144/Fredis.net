module Disruptor

open System.Threading




[<Literal>]
let initialSeqVal = -1L


[<Literal>]
let private nSpin = 8


(*
     needed to implement padded Sequence in C# because classes with mutable members are incompatible with the StructLayout attribute in F#

    [<StructLayout(LayoutKind.Explicit, Size = 128)>]
    type Sequence2 () =          // only structs and classes without primary constructors may be given the struct layout attribute
        [<FieldOffset(0)>]
        let mutable seqVal:int64 // this definition can only be used in a type with a primary constructor
    
*)

type Sequence = Padded.Sequence


(*
    ProducerWaitCAS

    1. contend with other producers to get a sequence value
    2. wait for that slot to become free
    3. return that slot value only
*)

let ProducerWaitCAS( bufSize, waitingSeq:Sequence, targetSeq:Sequence) =
    let mutable requestSeqVal   = initialSeqVal
    let mutable claimed         = false

    // claim a slot to write in, competing with the other producers
    while not claimed do
        let waitingSeqVal = Thread.VolatileRead ( &(waitingSeq._value)) 
        requestSeqVal <- (waitingSeqVal + 1L)
        let origSeqVal = Interlocked.CompareExchange( & waitingSeq._value, requestSeqVal, waitingSeqVal )
        claimed <- (origSeqVal = waitingSeqVal) // meaning prodSeq._value did not change before the CAS op 

    let claimedSeqVal = requestSeqVal

    // wait until the client has read enough to allow room to write
    let mutable targetSeqVal = Thread.VolatileRead ( &(targetSeq._value))
    while (claimedSeqVal - targetSeqVal) > bufSize do  
        Thread.SpinWait(nSpin)
        targetSeqVal <- Thread.VolatileRead ( &(targetSeq._value))
    
    claimedSeqVal


// single producer single consumer producer wait func
// not used in fredis
let ProducerWait( bufSize, waitingSeq:Sequence, targetSeq: Sequence) =
    let requestSeqVal = Thread.VolatileRead ( &waitingSeq._value )
    let mutable targetSeqVal = Thread.VolatileRead ( & targetSeq._value )
    while (requestSeqVal - targetSeqVal) > bufSize do
        System.Threading.Thread.SpinWait(nSpin)
        targetSeqVal <- Thread.VolatileRead ( &(targetSeq._value))
    let lastProdPosWritten = requestSeqVal - 1L
    lastProdPosWritten + bufSize - (lastProdPosWritten - targetSeqVal)




let ConsumerWait( waitingSeqVal:int64, targetSeq: Sequence) =
    let mutable targetSeqVal = Thread.VolatileRead ( &targetSeq._value )
    while waitingSeqVal = targetSeqVal do 
        System.Threading.Thread.SpinWait(nSpin)
        targetSeqVal <- Thread.VolatileRead ( &(targetSeq._value))
    targetSeqVal //consumer can read up to the producer sequence to get freeUpTo

