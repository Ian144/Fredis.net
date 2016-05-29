module SocAsyncEventArgFuncs


open System
open System.Net.Sockets
open System.Threading.Tasks




// TODO use OkContinuation in UserToken, maybe 
//type OkContinuation = 
//    | BytesCont         of byte array -> Unit
//    | ContUnit          of () -> ()
//    | SingleByteCont    of byte ->Unit

// reads can 
//  read more than required by the current op
//  be too small for the current client op, so multiple reads are required


[<NoEquality;NoComparison>]
type UserToken = {
    mutable Socket: Socket
    mutable ClientBuf: byte[]   // consider a struct holding both ClientBuf and ClientBufPos
    mutable ClientBufPos: int   // used when multiple reads are required to fully populate a client buffer
    mutable SaeaBufStart: int   // begining of the unread, by the client, part of the saea buffer (inclusive). Reads often pull in more the the current op requires, but its needed by the next op
    mutable SaeaBufEnd: int     // end of the unread (by the client) part of the saea buffer (exclusive)
    SaeaBufSize: int            // the size of that part of the shared saea buffer available to each saea
    //mutable Continuation: SocketAsyncEventArgs -> unit  // the continuation fired when socketAsyncEventArgs complete
    mutable Continuation: int  // the continuation fired when socketAsyncEventArgs complete
    BufList: System.Collections.Generic.List<byte[]>    // used when loading an unknown number of bytes, such as when reading until CRLF
    mutable okContBytes:byte array->Unit                // as received from Async.fromContinuations for read operations
    mutable okContUnit:Unit->Unit                       // as received from Async.fromContinuations for send operations
    mutable exnCont:exn->Unit                           // as received from Async.fromContinuations
    mutable cancCont:OperationCanceledException->Unit   // as received from Async.fromContinuations
    }


let cProcessSend = 0
let cProcessReceive = 1
let cProcessReceiveUntilCRLF = 2
let cProcessReceiveUntilCRLFEatFirstByte = 3
let cProcessReceiveEatCRLF = 4

//let OnClientIOCompleted (saea:SocketAsyncEventArgs) =
//    let ut = saea.UserToken :?> UserToken
//    ut.Continuation saea




[<NoEquality;NoComparison>]
type IFredisStreamSource =
    abstract AsyncReadUntilCRLF : unit -> Async<byte[]>
    abstract AsyncReadNBytes : int -> Async<byte[]>
    abstract AsyncReadByte : unit -> Async<byte>
    abstract AsyncEatCRLF: unit -> Async<unit> // could be just a convenience wrapper arount AsyncReadN, but usually should be able to avoid array alloc and copying, when the CRLF has been read from the socket but not consumed by RESP processing


[<NoEquality;NoComparison>]
type IFredisStreamSink =
    abstract AsyncWrite : byte[] -> Async<unit>
    abstract AsyncFlush : unit -> Async<unit>



type OpStatus = OpComplete|AnotherOpRequired




let rec ProcessReceive (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    let bytesTransferred = saea.BytesTransferred
    let bytesRequired = ut.ClientBuf.Length - ut.ClientBufPos


 //    let str = (SubBufToStr saea.Buffer saea.Offset  bytesTransferred )
   
    let opStatus = 
            if bytesTransferred >= bytesRequired
            then OpComplete        
            else AnotherOpRequired

    match saea.SocketError, bytesTransferred, opStatus with
    | SocketError.Success, 0, _ -> 
            // TODO where can the existing cancellation token be found
            let cancEx = new OperationCanceledException()
            ut.cancCont cancEx
    
    | SocketError.Success, _, OpComplete ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset, ut.ClientBuf, ut.ClientBufPos, bytesRequired)
            let clientBuf = ut.ClientBuf
            let okContBytes = ut.okContBytes
            ut.ClientBufPos <- ut.ClientBuf.Length
            // may have read in data required for the next client op, i.e. more than required for the current op, so mark the range
            ut.SaeaBufStart <- bytesRequired    
            ut.SaeaBufEnd   <- bytesTransferred
            okContBytes clientBuf
    
    | SocketError.Success, _, AnotherOpRequired ->
            Buffer.BlockCopy (saea.Buffer, saea.Offset, ut.ClientBuf, ut.ClientBufPos, bytesTransferred)
            ut.ClientBufPos <- ut.ClientBufPos + bytesTransferred
            ut.SaeaBufStart <- ut.SaeaBufSize
            ut.SaeaBufEnd   <- ut.SaeaBufSize
            let ioPending = ut.Socket.ReceiveAsync saea
            if not ioPending then
                ProcessReceive(saea)
    
    | err, _, _ ->
            let msg = sprintf "received socket error: %O" err
            let ex = new Exception(msg)
            ut.exnCont ex



//type Found = Found of int | NotFound
//             
//// find CR idx between startIdx and before endIdx
//let private FindCR (buf:byte[]) startIdx endIdx = 
//    let mutable found = false
//    let mutable ctr = startIdx
//    while (not found) && (ctr < endIdx) do
//        if buf.[ctr] = 13uy then 
//            found <- true
//        else
//            ctr <- ctr + 1
//    if found 
//        then Found ctr
//        else NotFound


let private FindCR (buf:byte[]) startIdx endIdx = 
    let mutable found = false
    let mutable ctr = startIdx
    while (not found) && (ctr < endIdx) do
        if buf.[ctr] = 13uy then 
            found <- true
        else
            ctr <- ctr + 1
    found, ctr




    // todo: this may be slow, possibly involving multiple allocations
let private flattenListBuf listBufs = [| for b in listBufs do yield! b |]
    

let rec ProcessReceiveUntilCRLF (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    let bytesTransferred = saea.BytesTransferred
    let startIdx = saea.Offset 
    let endIdx = bytesTransferred + startIdx
    ut.SaeaBufStart <- saea.Offset
    ut.SaeaBufEnd <- bytesTransferred + saea.Offset
    let crFound, idx = FindCR saea.Buffer startIdx endIdx

    match saea.SocketError, crFound, idx, bytesTransferred with
    | SocketError.Success, _, _, 0 -> 
            // client has disconnected
            ut.BufList.Clear()
            // TODO confirm zero bytes received with the client has disconnected
            // TODO where can the existing cancellation token be found
            failwith "where can the existing cancellation token be found?"
            let cancEx = OperationCanceledException ()
            ut.cancCont cancEx

    | SocketError.Success, true, crIdx, _ ->
            // CRLF found
            let crInLastByte = (crIdx = (endIdx - 1))

            if crInLastByte then
                // the LF in CRLF has not been read in yet, so doing another read and eating the first char
                ut.Continuation <- cProcessReceiveUntilCRLFEatFirstByte
                let len = crIdx - ut.SaeaBufStart
                let tmpBuf = Array.zeroCreate<byte> len
                Buffer.BlockCopy( saea.Buffer, startIdx, tmpBuf, 0, len)
                ut.BufList.Add tmpBuf
                ut.SaeaBufStart <- ut.SaeaBufSize 
                ut.SaeaBufEnd <- ut.SaeaBufSize
                let ioPending = ut.Socket.ReceiveAsync(saea)
                if not ioPending then
                    ProcessReceiveUntilCRLFEatFirstByte(saea)
            else
                ut.ClientBufPos <- ut.ClientBuf.Length
                // there may be data read-in that will be reqired by the next client op
                ut.SaeaBufStart <- crIdx + 2 - saea.Offset  // +2 to ingore the CRLF
                ut.SaeaBufEnd   <- endIdx - saea.Offset
                let len = crIdx - startIdx
                let buf = Array.zeroCreate<byte> len
                Buffer.BlockCopy(saea.Buffer, saea.Offset, buf, 0, len)
                ut.BufList.Add buf 
                let bufAll = flattenListBuf ut.BufList
                ut.BufList.Clear ()
                ut.okContBytes bufAll
    
    | SocketError.Success, false, _, _ ->
            // CRLF not found, continue reading until it is
            let len = ut.SaeaBufEnd - ut.SaeaBufStart
            let buf = Array.zeroCreate<byte> len
            Buffer.BlockCopy( saea.Buffer, startIdx, buf, 0, len)
            ut.SaeaBufStart <- ut.SaeaBufSize // the entire saea buffer has now been read into the client buffer, so set the saea buf to empty
            ut.SaeaBufEnd <- ut.SaeaBufSize
            ut.BufList.Add buf
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessReceiveUntilCRLF(saea)
    | err, _, _, _  ->
            ut.BufList.Clear()
            let msg = sprintf "receive socket error: %O" err
            let ex = Exception msg
            ut.exnCont ex

// called when a CR has been read as the last byte of a previous read
// assuming the first byte is LF, todo: dont assume
and ProcessReceiveUntilCRLFEatFirstByte (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    let bytesTransferred = saea.BytesTransferred
    ut.SaeaBufStart <- 0
    ut.SaeaBufEnd <- bytesTransferred

    let fb = saea.Buffer.[saea.Offset] 
    //assert ( fb = 10uy || fb = 42uy)
    match saea.SocketError, bytesTransferred with
    | SocketError.Success, 0 -> 
            ut.BufList.Clear()
            // TODO confirm zero bytes received with the client has disconnected
            // TODO where can the existing cancellation token be found
            failwith "where can the existing cancellation token be found"
            let cancEx = OperationCanceledException ()
            ut.cancCont cancEx
    | SocketError.Success, _   ->
            // eat the first char and collate the result
            ut.SaeaBufStart <- 1   
            let bufArr  = ut.BufList |> Seq.toArray                
            let buf = 
                    if bufArr.Length = 1 
                    then bufArr.[0]
                    else 
                        // this will be slow, involving multiple allocations, but hopefully is rare, 
                        // todo: check the array comprension does not do unnesseccary allocations
                        [|  for b in bufArr do
                            yield! b |]
            ut.BufList.Clear ()
            ut.okContBytes buf
    | err, _ ->
            ut.BufList.Clear()
            let msg = sprintf "receive socket error: %O" err
            let ex = Exception msg
            ut.exnCont ex


let ProcessReceiveEatCRLF (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    let bytesTransferred = saea.BytesTransferred
    ut.SaeaBufStart <- 0
    ut.SaeaBufEnd <- bytesTransferred

    match saea.SocketError, bytesTransferred with
    | SocketError.Success, 0 -> 
            ut.BufList.Clear()
            // TODO confirm zero bytes received with the client has disconnected
            // TODO where can the existing cancellation token be found
            failwith "where can the existing cancellation token be found"
            let cancEx = OperationCanceledException ()
            ut.cancCont cancEx
    | SocketError.Success, 1   ->
            // the hopefully rare case where only one byte was read, which should be CR, kick off another read for the LF
            let fb = saea.Buffer.[saea.Offset] 
            assert ( fb = 10uy )
            ut.SaeaBufStart <- ut.SaeaBufSize
            ut.SaeaBufEnd   <- ut.SaeaBufSize
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessReceiveUntilCRLFEatFirstByte(saea)
    | SocketError.Success, _   ->
            let fb = saea.Buffer.[saea.Offset] 
            let sb = saea.Buffer.[saea.Offset + 1] 
            assert ( fb = 13uy && sb = 10uy )
            ut.SaeaBufStart <- 2    // eat the first two chars
            ut.okContUnit ()
    | err, _ ->
            ut.BufList.Clear()
            let msg = sprintf "receive socket error: %O" err
            let ex = Exception msg
            ut.exnCont ex



// requires a buffer to be supplied, enabling pre-allocated buffers to be reused
let AsyncRead (saea:SocketAsyncEventArgs) (dest:byte []) : Async<byte[]> =
    let AsyncReadReadInner (okCont, exnCont, cancCont) =
        let ut = saea.UserToken :?> UserToken
        ut.okContBytes <- okCont
        ut.exnCont <- exnCont
        ut.Continuation <- cProcessReceive
        ut.cancCont <- cancCont
        ut.ClientBuf <- dest
        ut.ClientBufPos <- 0   // AsyncRead will always read into the client Buffer starting at index zero

        let availableBytes = ut.SaeaBufEnd - ut.SaeaBufStart // some bytes may have been read-in already, by a previous read operation
        //assert (saea.Count = ut.SaeaBufSize)
        let opStatus = 
                if availableBytes >= dest.Length
                then OpComplete        
                else AnotherOpRequired

        match availableBytes, opStatus with
        | 0, _ ->  // there are no bytes available, 
                ut.SaeaBufStart <- ut.SaeaBufSize
                ut.SaeaBufEnd   <- ut.SaeaBufSize
                let ioPending = ut.Socket.ReceiveAsync(saea)
                if not ioPending then
                    ProcessReceive(saea)
        
        | availBs, OpComplete ->
                // able to satisfy the entire read request from bytes already available
                Buffer.BlockCopy(saea.Buffer, saea.Offset + ut.SaeaBufStart, dest, 0, dest.Length) 
                ut.SaeaBufStart <- ut.SaeaBufStart + dest.Length // mark the new begining of the unread bytes
                ut.okContBytes dest
        
        | availBs, AnotherOpRequired  ->
                Buffer.BlockCopy(saea.Buffer, saea.Offset + ut.SaeaBufStart, dest, 0, availableBytes)
                ut.ClientBufPos <- availableBytes // availableBytes have been written into the client array (aka 'dest'), so the first unread index is 'availableBytes' because dest is zero based
                ut.SaeaBufStart <- ut.SaeaBufSize
                ut.SaeaBufEnd   <- ut.SaeaBufSize
                let ioPending = ut.Socket.ReceiveAsync(saea)
                if not ioPending then
                    ProcessReceive(saea)
    AsyncReadReadInner |> Async.FromContinuations
         


// does not require a buffer to be supplied
let AsyncRead2 (saea:SocketAsyncEventArgs) (len:int) : Async<byte[]> =
    let dest = Array.zeroCreate len // todo, consider how to avoid an alloc in AsyncRead2
    AsyncRead saea dest


// requires a buffer to be supplied, enabling pre-allocated buffers to be reused
let AsyncReadUntilCRLF (saea:SocketAsyncEventArgs) : Async<byte[]> =
    Async.FromContinuations <| fun (okCont, exnCont, cancCont) -> 
        let ut = saea.UserToken :?> UserToken
        ut.okContBytes <- okCont
        ut.exnCont <- exnCont
        ut.cancCont <- cancCont
        let ut = saea.UserToken :?> UserToken
        ut.Continuation <- cProcessReceiveUntilCRLF
        let endIdx = ut.SaeaBufEnd + saea.Offset
        let startIdx = ut.SaeaBufStart + saea.Offset
        let crFound, idx = FindCR saea.Buffer startIdx endIdx

        match crFound, idx with
        | true, crIdx when crIdx < (endIdx - 1)   ->   
                // CR found and there is at least one more char to read without doing another receive
                // hopefully this is the common case, as others are potentially slow
                let len = (crIdx - startIdx) // -1 as the CR is being ignored
                let arrOut = Array.zeroCreate len
                Buffer.BlockCopy( saea.Buffer, startIdx, arrOut,0, len)
                ut.SaeaBufStart <- ut.SaeaBufStart + len + 2 // +2 so as to go past the CRLF
                //no change to ut.SaeaBufEnd should be made, it still marks the end of valid bytes in the saea buffer
                ut.okContBytes arrOut
        | true, crIdx ->  
                // the CR is the last char in the saea buffer, so read again, and throw away the first byte which should be LF
                ut.Continuation <- cProcessReceiveUntilCRLFEatFirstByte
                let len = (ut.SaeaBufEnd - ut.SaeaBufStart) - 1 // -1 to ignore the CR at the end
                //assert (len > 0)
                let buf1 = Array.zeroCreate<byte> len
                Buffer.BlockCopy( saea.Buffer, startIdx, buf1, 0, len)
                ut.BufList.Add buf1
                ut.SaeaBufStart <- ut.SaeaBufSize // the entire saea buffer has now been read into the client buffer, so set the saea buf to empty
                ut.SaeaBufEnd <- ut.SaeaBufSize
                let ioPending = ut.Socket.ReceiveAsync(saea)
                if not ioPending then
                    ProcessReceiveUntilCRLFEatFirstByte(saea)
        | false, _ ->  
                // CRLF not found, continue reading until it is
                let len = ut.SaeaBufEnd - ut.SaeaBufStart
                if len > 0 then
                    let buf1 = Array.zeroCreate<byte> len
                    Buffer.BlockCopy( saea.Buffer, startIdx, buf1, 0, len)
                    ut.BufList.Add buf1
                ut.SaeaBufStart <- ut.SaeaBufSize // the entire saea buffer has now been read into the client buffer, so set the saea buf to empty
                ut.SaeaBufEnd <- ut.SaeaBufSize
                let ioPending = ut.Socket.ReceiveAsync(saea)
                if not ioPending then
                    ProcessReceiveUntilCRLF(saea)



let AsyncReadByte (saea:SocketAsyncEventArgs) : Async<byte> =
    async{   // nested async to convert Async<byte[]> to Async<byte>
        let! bs = AsyncRead2 saea 1
        return bs.[0]
    }


// AsyncReadByte2 but allows the use of a preallocated buffer, there will be one per client
let AsyncReadByte2 (saea:SocketAsyncEventArgs) (buf:byte[]): Async<byte> =
    async{  // nested async to convert Async<byte[]> to Async<byte>
        let! bs = AsyncRead saea buf 
        return bs.[0]
    }


let AsyncEatCRLF (saea:SocketAsyncEventArgs) : Async<unit> =
    Async.FromContinuations <| fun (okCont, exnCont, cancCont) -> 
        let ut = saea.UserToken :?> UserToken
        ut.okContUnit <- okCont
        ut.exnCont <- exnCont
        ut.cancCont <- cancCont
        let availableBytes = ut.SaeaBufEnd - ut.SaeaBufStart // some bytes may have been read-in already, by a previous read operation

        // want the common cheap case handled first, so not useing a match expression
        if availableBytes > 1 then
            // hopefully the common case, where asking for 2 bytes does not require another read
            let crPos = saea.Offset + ut.SaeaBufStart
            let lfPos = crPos + 1
            if not (saea.Buffer.[crPos] = 13uy && saea.Buffer.[lfPos] = 10uy ) then failwith "AsyncEatCRLF next bytes are not CRLF"
            ut.SaeaBufStart <- ut.SaeaBufStart + 2 // mark the new begining of the unread bytes
            ut.okContUnit ()
        else  
            // the hopefully rare case where asking for 2 bytes requires another socket read
            if availableBytes = 1 then
                // the CR is the last char in the saea buffer, throw it away and read again, then throw away the first byte which should be LF
                ut.Continuation <- cProcessReceiveUntilCRLFEatFirstByte
                ut.SaeaBufStart <- ut.SaeaBufSize // the entire saea buffer has now been read into the client buffer, so set the saea buf to empty
                ut.SaeaBufEnd <- ut.SaeaBufSize
                let ioPending = ut.Socket.ReceiveAsync(saea)
                if not ioPending then
                    ProcessReceiveUntilCRLFEatFirstByte(saea)
            else
                // CRLF is not in the buffer, should be the next two chars to be read
                ut.Continuation <- cProcessReceiveEatCRLF
                ut.SaeaBufStart <- ut.SaeaBufSize // the entire saea buffer has now been read into the client buffer, so set the saea buf to empty
                ut.SaeaBufEnd <- ut.SaeaBufSize
                let ioPending = ut.Socket.ReceiveAsync(saea)
                if not ioPending then
                    ProcessReceiveEatCRLF saea



let rec private SetupSend (saea:SocketAsyncEventArgs) ut =
    saea.SetBuffer(saea.Offset + ut.SaeaBufStart, ut.SaeaBufEnd - ut.SaeaBufStart)
    let ioPending = ut.Socket.SendAsync(saea)
    if not ioPending then
        ProcessSend(saea)                                    
and ProcessSend (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    let bytesTransferred = saea.BytesTransferred
    ut.SaeaBufStart <- ut.SaeaBufStart + bytesTransferred
    let saeaBufLenRemaining = ut.SaeaBufEnd - ut.SaeaBufStart
    match saea.SocketError, saeaBufLenRemaining  with
    | SocketError.Success, 0 -> 
        ut.SaeaBufStart <- 0
        ut.SaeaBufEnd <- 0
        let clientBufLenRemaining = ut.ClientBuf.Length - ut.ClientBufPos
        match clientBufLenRemaining > 0, clientBufLenRemaining <= ut.SaeaBufSize with
        | false, _      ->  // the send is complete
            saea.SetBuffer(saea.Offset, ut.SaeaBufSize) // restore the saea buffer so that all of the buffer will be used for the next op
            ut.okContUnit ()
        | true,  true   -> // enough space in the saeaBuffer to send the unsent part of the client buffer
            Buffer.BlockCopy(ut.ClientBuf, ut.ClientBufPos, saea.Buffer, saea.Offset, clientBufLenRemaining)                        
            ut.SaeaBufStart <- 0
            ut.SaeaBufEnd <- clientBufLenRemaining
            ut.ClientBufPos <- ut.ClientBufPos + clientBufLenRemaining
            //assert (ut.ClientBufPos = ut.ClientBuf.Length)
            SetupSend saea ut
        | true,  false  ->  // NOT enough space in the saeaBuffer to send all of the unsent clientBuf
            Buffer.BlockCopy(ut.ClientBuf, ut.ClientBufPos, saea.Buffer, saea.Offset, ut.SaeaBufSize)                        
            ut.SaeaBufStart <- 0
            ut.SaeaBufEnd <- ut.SaeaBufSize
            ut.ClientBufPos <- ut.ClientBufPos +  ut.SaeaBufSize
            SetupSend saea ut
    | SocketError.Success, saeaBufLenRemaining when saeaBufLenRemaining > 0 -> 
            // not all of the bytes in the saea buffer were sent
            // send them, atm not shifting the unsent bytes to the begining of the saea buf and copying client bytes to the now available space
            SetupSend saea ut
    | err   ->
            let msg = sprintf "send socket error: %O" err
            let ex = new Exception(msg)
            // send is over, restore the saea buffer so that all of it is used for subsequent reads
            saea.SetBuffer(saea.Offset, ut.SaeaBufSize)
            ut.exnCont ex



let AsyncWrite (saea:SocketAsyncEventArgs) (bs:byte[]) : Async<unit> =
    Async.FromContinuations <| fun (okCont, exnCont, cancCont) ->
        let ut = saea.UserToken :?> UserToken
        ut.okContUnit <- okCont
        ut.exnCont <- exnCont
        ut.cancCont <- cancCont
        let saeaBufSpaceAvailable = ut.SaeaBufSize - ut.SaeaBufEnd
        match bs.Length <= saeaBufSpaceAvailable with
        | true  ->
            // there is enough space available in the saea buf to store the contents of the client buf, which will be sent later
            Buffer.BlockCopy(bs, 0, saea.Buffer, saea.Offset + ut.SaeaBufEnd, bs.Length)
            ut.SaeaBufEnd <- ut.SaeaBufEnd + bs.Length
            ut.ClientBuf <- bs
            ut.ClientBufPos <- bs.Length // all bytes in bs have been copied to the saea buffwer
            okCont ()
        | false ->
            // there is not enough space in the saea buf to accomodate bs
            ut.ClientBuf <- bs
            ut.Continuation <- cProcessSend
            ut.okContUnit <- okCont
            Buffer.BlockCopy(bs, 0, saea.Buffer, saea.Offset + ut.SaeaBufEnd, saeaBufSpaceAvailable)
            ut.SaeaBufEnd <- ut.SaeaBufSize // 
            ut.ClientBufPos <- saeaBufSpaceAvailable  // no part of bs will have been sent before this call to AsyncWrite
            SetupSend saea ut



let AsyncFlush (saea:SocketAsyncEventArgs) : Async<unit> =
    Async.FromContinuations <| fun (okCont, exnCont, cancCont) -> 
        let ut = saea.UserToken :?> UserToken    
        ut.okContUnit <- okCont
        ut.exnCont <- exnCont
        ut.cancCont <- cancCont
        let numToSend = ut.SaeaBufEnd - ut.SaeaBufStart
//        printfn "sending: %d bytes" numToSend
        if numToSend = 0 then
            ut.okContUnit ()
        else
            ut.Continuation <- cProcessSend
            SetupSend saea ut



let Reset (saea:SocketAsyncEventArgs)=
    let ut = saea.UserToken :?> UserToken    
    ut.SaeaBufEnd <- 0
    ut.SaeaBufStart <- 0
    ut.ClientBuf <- [||]
    ut.ClientBufPos <- 0
//    ut.okContBytes <- ignore
//    ut.okContUnit <- ignore
//    ut.exnCont <- ignore 
//    ut.cancCont <- ignore 
//    Array.Clear( saea.Buffer, saea.Offset, ut.SaeaBufSize )
    assert (saea.Count = ut.SaeaBufSize)
    ut.BufList.Clear()


let OnClientIOCompleted (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    match ut.Continuation with
    | 0 -> ProcessSend saea
    | 1 -> ProcessReceive saea
    | 2 -> ProcessReceiveUntilCRLF saea
    | 3 -> ProcessReceiveUntilCRLFEatFirstByte saea
    | 4 -> ProcessReceiveEatCRLF saea
    | _ -> failwith "invalid ut.Continuation"



type SaeaStreamSource (saea:SocketAsyncEventArgs) =
    interface IFredisStreamSource with
        override this.AsyncReadNBytes len = AsyncRead2 saea len
        override this.AsyncReadUntilCRLF () = AsyncReadUntilCRLF saea
        override this.AsyncReadByte () = AsyncReadByte saea
        override this.AsyncEatCRLF () =  AsyncEatCRLF saea


[<NoEquality;NoComparison>]
type SaeaStreamSink (saea:SocketAsyncEventArgs) =
    interface IFredisStreamSink with
        member this.AsyncWrite bs = AsyncWrite saea bs
        member this.AsyncFlush () = AsyncFlush saea


