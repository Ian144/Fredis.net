module SocAsyncEventArgFuncs


open System
open System.Net.Sockets
open System.Threading.Tasks








// TODO use OkContinuation in UserToken, maybe 
//type OkContinuation = 
//    | BytesCont         of byte array -> Unit
//    | ContUnit          of () -> ()
//    | SingleByteCont    of byte ->Unit


[<NoEquality;NoComparison>]
type UserToken = {
    mutable Socket: Socket
    mutable ClientBuf: byte[]   // consider a struct holding both ClientBuf and ClientBufPos
    mutable ClientBufPos: int
    mutable SaeaBufStart: int   // offsets into each saea's section of the shared buffer, does not take into account the offset of the shared section
    mutable SaeaBufEnd: int
    SaeaBufSize: int    // the size of that part of the  shared buffer available to each saea
    SaeaBufOffset:int
    mutable Continuation: SocketAsyncEventArgs -> unit  // the continuation fired when socketAsyncEventArgs complete
    BufList: System.Collections.Generic.List<byte[]>    // used when loading an unknown number of bytes, such as when reading up to a delimiter
    mutable okContSingleByte:byte ->Unit                // as received from Async.fromContinuations for read operations
    mutable okContBytes:byte array->Unit                // as received from Async.fromContinuations for read operations
    mutable okContUnit:Unit->Unit                       // as received from Async.fromContinuations for send operations
    mutable exnCont:exn->Unit                           // as received from Async.fromContinuations
    mutable cancCont:OperationCanceledException->Unit   // as received from Async.fromContinuations
    }





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


type AnotherOpRequired = OpComplete|AnotherOpRequired

let AnotherRequiredFunc available required = 
    if available >= required
    then OpComplete        
    else AnotherOpRequired

let SubBufToStr buf startIdx len = 
    let tmp = Array.zeroCreate<byte> len
    Buffer.BlockCopy( buf, startIdx, tmp, 0, len )
    let str = Utils.BytesToStr tmp
    str.Replace("\r\n", "..")

let rec ProcessReceive (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    let bytesTransferred = saea.BytesTransferred
    let bytesRequired = ut.ClientBuf.Length - ut.ClientBufPos
   
    let str = (SubBufToStr saea.Buffer saea.Offset  bytesTransferred )
    printfn "ProcessReceive, bytesTransferred: %d\n%s" bytesTransferred  str
   
    match saea.SocketError, bytesTransferred, (AnotherRequiredFunc bytesTransferred bytesRequired) with
    | SocketError.Success, 0, _ -> 
            // TODO confirm zero bytes received when the client has disconnected
            // TODO where can the existing cancellation token be found
            failwith "where can the existing cancellation token be found"
            let cancEx = new OperationCanceledException()
            ut.cancCont cancEx
    
    | SocketError.Success, _, OpComplete ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset, ut.ClientBuf, ut.ClientBufPos, bytesRequired)
            ut.ClientBufPos <- ut.ClientBuf.Length
            ut.SaeaBufStart <- bytesRequired
            ut.SaeaBufEnd   <- bytesTransferred
            ut.okContBytes ut.ClientBuf
    
    | SocketError.Success, _, AnotherOpRequired ->
            printfn "ProcessReceive slow path, %d received, %d required" bytesTransferred bytesRequired
            Buffer.BlockCopy(saea.Buffer, saea.Offset, ut.ClientBuf, ut.ClientBufPos, bytesTransferred)
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


             
// find CR idx between startIdx and before endIdx
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
    let crFound, crIdx = FindCR saea.Buffer startIdx endIdx

    match saea.SocketError, crFound, crIdx, bytesTransferred with
    | SocketError.Success, _, _, 0 -> 
            ut.BufList.Clear()
            // TODO confirm zero bytes received with the client has disconnected
            // TODO where can the existing cancellation token be found
            failwith "where can the existing cancellation token be found?"
            let cancEx = OperationCanceledException ()
            ut.cancCont cancEx
    | SocketError.Success, true, crIdx,_  when crIdx < (endIdx - 1) ->
            ut.ClientBufPos <- ut.ClientBuf.Length
            ut.SaeaBufStart <- crIdx + 2 - saea.Offset  // +2 to ingore the CRLF
            ut.SaeaBufEnd   <- endIdx - saea.Offset
            let len = crIdx - startIdx
            let buf = Array.zeroCreate<byte> len
            Buffer.BlockCopy(saea.Buffer, saea.Offset, buf, 0, len)
            ut.BufList.Add buf 
            let bufAll = flattenListBuf ut.BufList
            ut.BufList.Clear ()
            ut.okContBytes bufAll
    | SocketError.Success, true, crIdx, _  when crIdx = (endIdx - 1) ->
            printfn "ProcessReceiveUntilCRLF slow path, need ProcessReceiveUntilCRLFEatFirstByte"
            ut.Continuation <- ProcessReceiveUntilCRLFEatFirstByte
            let len = crIdx - ut.SaeaBufStart
            let buf = Array.zeroCreate<byte> len
            Buffer.BlockCopy( saea.Buffer, startIdx, buf, 0, len)
            ut.BufList.Add buf
            ut.SaeaBufStart <- ut.SaeaBufSize   // there is still the CR in the last element to read
            ut.SaeaBufEnd <- ut.SaeaBufSize
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessReceiveUntilCRLFEatFirstByte(saea)
    | SocketError.Success, true, crIdx, _  when crIdx > (endIdx - 1) ->
            failwith "invalid carriage return index found"
    | SocketError.Success, false, _, _  ->
            // CRLF not found, continue reading until it is
            printfn "ProcessReceiveUntilCRLF slow path, CRLF not found"
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

    printfn "ProcessReceiveUntilCRLFEatFirstByte firstByte: %d, trans: %d" saea.Buffer.[saea.Offset] bytesTransferred
    let fb = saea.Buffer.[saea.Offset] 
    assert ( fb = 10uy || fb = 42uy)
    printfn "ProcessReceiveUntilCRLFEatFirstByte, %d bytes" bytesTransferred
    match saea.SocketError, bytesTransferred with
    | SocketError.Success, 0 -> 
            ut.BufList.Clear()
            // TODO confirm zero bytes received with the client has disconnected
            // TODO where can the existing cancellation token be found
            failwith "where can the existing cancellation token be found"
            let cancEx = OperationCanceledException ()
            ut.cancCont cancEx
    | SocketError.Success, _   ->
//            printfn "ProcessReceiveUntilCRLFEatFirstByte slow path"
            // eat the first char and collate the result
            ut.SaeaBufStart <- 1   
            let bufArr  = ut.BufList |> Seq.toArray                
            let buf = 
                    if bufArr.Length = 1 
                    then bufArr.[0]
                    else // todo: this may be slow, possibly involving multiple allocations, but hopefully is rare
                        [|  for b in bufArr do
                            yield! b |]
            ut.BufList.Clear ()
            ut.okContBytes buf
    | err, _ ->
            ut.BufList.Clear()
            let msg = sprintf "receive socket error: %O" err
            let ex = Exception msg
            ut.exnCont ex


// requires a buffer to be supplied, enabling pre-allocated buffers to be reused
let AsyncRead (saea:SocketAsyncEventArgs) (dest:byte []) : Async<byte[]> =
    Async.FromContinuations <| fun (okCont, exnCont, cancCont) -> 
        let ut = saea.UserToken :?> UserToken
        ut.okContBytes <- okCont
        ut.exnCont <- exnCont
        ut.cancCont <- cancCont
        ut.ClientBuf <- dest
        ut.ClientBufPos <- 0   // AsyncRead will always read into the client Buffer starting at index zero
        ut.Continuation <- ProcessReceive
        let availableBytes = ut.SaeaBufEnd - ut.SaeaBufStart // some bytes may have been read-in already, by a previous read operation
        match availableBytes with
        | _ when availableBytes >= dest.Length ->
                Buffer.BlockCopy(saea.Buffer, saea.Offset + ut.SaeaBufStart, dest, 0, dest.Length) // able to satisfy the entire read request from bytes already available
                ut.SaeaBufStart <- ut.SaeaBufStart + dest.Length // mark the new begining of the unread bytes
                ut.okContBytes dest
        | _ when availableBytes > 0 ->
                printfn "AsyncRead slow path, %d bytes available, %d required" availableBytes dest.Length
                Buffer.BlockCopy(saea.Buffer, saea.Offset + ut.SaeaBufStart, dest, 0, availableBytes)
                ut.ClientBufPos <- availableBytes // availableBytes have been written into the client array (aka 'dest'), so the first unread index is 'availableBytes' because dest is zero based
                ut.SaeaBufStart <- ut.SaeaBufSize
                ut.SaeaBufEnd   <- ut.SaeaBufSize
                let ioPending = ut.Socket.ReceiveAsync(saea)
                if not ioPending then
                    ProcessReceive(saea)
        | _ ->  // probably the first read for a new RESP msg
//                printfn "AsyncRead slow path"
                printfn "AsyncRead fresh read"
                ut.SaeaBufStart <- ut.SaeaBufSize
                ut.SaeaBufEnd   <- ut.SaeaBufSize
                let ioPending = ut.Socket.ReceiveAsync(saea)
                if not ioPending then
                    ProcessReceive(saea)
            



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
        ut.Continuation <- ProcessReceiveUntilCRLF
        let endIdx = ut.SaeaBufEnd + saea.Offset
        let startIdx = ut.SaeaBufStart + saea.Offset
        let crFound, crIdx = FindCR saea.Buffer startIdx endIdx
        // todo: check that LF follows CR
        //printfn "%d to %d: %d "  ut.SaeaBufStart  ut.SaeaBufEnd (crIdx-saea.Offset)

        match crFound, crIdx  with
        | true, crIdx when crIdx < (endIdx - 1)   ->   
                printfn "AsyncReadUntilCRLF fast path"            
                // CR found and there is at least one more char to read without doing another receive
                // hopefully this is the common case, as others are potentially slow
                let len = (crIdx - startIdx) // -1 as the CR is being ignored
                let arrOut = Array.zeroCreate len
                Buffer.BlockCopy( saea.Buffer, startIdx, arrOut,0, len)
                ut.SaeaBufStart <- ut.SaeaBufStart + len + 2 // +2 so as to go past the CRLF
                //no change to ut.SaeaBufEnd should be made, it still marks the end of valid bytes in the saea buffer
                ut.okContBytes arrOut
        | true, _   ->  
                printfn "AsyncReadUntilCRLF slow path CR is the last char"
                // the CR is the last char in the saea buffer, so read again, and throw away the first byte which should be LF
                ut.Continuation <- ProcessReceiveUntilCRLFEatFirstByte
                let len = (ut.SaeaBufEnd - ut.SaeaBufStart) - 1 // -1 to ignore the CR at the end
                assert (len > 0)
                let buf1 = Array.zeroCreate<byte> len
                Buffer.BlockCopy( saea.Buffer, startIdx, buf1, 0, len)
                ut.BufList.Add buf1
                ut.SaeaBufStart <- ut.SaeaBufSize // the entire saea buffer has now been read into the client buffer, so set the saea buf to empty
                ut.SaeaBufEnd <- ut.SaeaBufSize
                let ioPending = ut.Socket.ReceiveAsync(saea)
                if not ioPending then
                    ProcessReceiveUntilCRLFEatFirstByte(saea)
        | false, _   ->  
                // CRLF not found, continue reading until it is
                printfn "AsyncReadUntilCRLF slow path, another read is required"
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
    async{
            let! bs = AsyncRead2 saea 1
            return bs.[0]
    }



let AsyncEatCRLF (saea:SocketAsyncEventArgs) : Async<unit> =
    Async.FromContinuations <| fun (okCont, exnCont, cancCont) -> 
        let ut = saea.UserToken :?> UserToken
        ut.okContUnit <- okCont
        ut.exnCont <- exnCont
        ut.cancCont <- cancCont
        let availableBytes = ut.SaeaBufEnd - ut.SaeaBufStart // some bytes may have been read-in already, by a previous read operation

        if availableBytes > 1 then
            // hopefully the common case, where asking for 2 bytes does not require array allocation
            let crPos = saea.Offset + ut.SaeaBufStart
            let lfPos = crPos + 1
            if not (saea.Buffer.[crPos] = 13uy && saea.Buffer.[lfPos] = 10uy ) then failwith "AsyncEatCRLF next bytes are not CRLF"
            ut.SaeaBufStart <- ut.SaeaBufStart + 2 // mark the new begining of the unread bytes
            ut.okContUnit ()
        else  // the hopefully rare case where asking for a 2 byes requires another socket read
            printfn "AsyncEatCRLF slow path, another read required"
            failwith "AsyncEatCRLF slow path not implemented"
//                async{
//                    let! bs = AsyncRead2 saea 2 // can i avoid the alloc here? maybe does not happen often so does it matter?
//                    if bs <>  [|13uy;10uy|] then failwith "AsyncEatCRLF next bytes are not CRLF"
//                    return ()
//                }




let rec private SetupSend (saea:SocketAsyncEventArgs) ut =
    let arr  = Array.zeroCreate<byte> (ut.SaeaBufEnd - ut.SaeaBufStart) 
    Buffer.BlockCopy(saea.Buffer, saea.Offset + ut.SaeaBufStart, arr, 0, ut.SaeaBufEnd - ut.SaeaBufStart )
    let str = arr |> Utils.BytesToStr
    let str2 = str.Replace("\r\n", "\\r\\n")
    saea.SetBuffer(ut.SaeaBufOffset + ut.SaeaBufStart, ut.SaeaBufEnd - ut.SaeaBufStart)
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
        | false, _      ->  // there is nothing left to send
            ut.okContUnit ()
        | true,  true   -> // enough space in the saeaBuffer to send the unsent part of the client buffer
            printfn "ProcessSend slow path, another send required, enough avail in saea buf"
            Buffer.BlockCopy(ut.ClientBuf, ut.ClientBufPos, saea.Buffer, saea.Offset, clientBufLenRemaining)                        
            ut.SaeaBufStart <- 0
            ut.SaeaBufEnd <- clientBufLenRemaining
            ut.ClientBufPos <- ut.ClientBufPos + clientBufLenRemaining
            assert (ut.ClientBufPos = ut.ClientBuf.Length)
            SetupSend saea ut
        | true,  false  ->  // NOT enough space in the saeaBuffer to send all of the unsent clientBuf
            printfn "ProcessSend slow path, another send required, more than saea buf size"
            Buffer.BlockCopy(ut.ClientBuf, ut.ClientBufPos, saea.Buffer, saea.Offset, ut.SaeaBufSize)                        
            ut.SaeaBufStart <- 0
            ut.SaeaBufEnd <- ut.SaeaBufSize
            ut.ClientBufPos <- ut.ClientBufPos +  ut.SaeaBufSize
            SetupSend saea ut
    | SocketError.Success, saeaBufLenRemaining when saeaBufLenRemaining > 0 -> 
            printfn "ProcessSend slow path, another send required"
            // not all of the bytes in the saea buffer were sent
            // send them, atm not shifting the unsent bytes to the begining of the saea buf and copying client bytes to the now available space
            //printfn "ProcessSend 4"
            SetupSend saea ut
    | err   ->
            let msg = sprintf "send socket error: %O" err
            let ex = new Exception(msg)
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
                printfn "AsyncWrite the slow path, send required - not enough space in buffer"
                // there is not enough space in the saea buf to accomodate bs
                ut.ClientBuf <- bs
                ut.Continuation <- ProcessSend
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
        if numToSend = 0 then
            saea.SetBuffer(ut.SaeaBufOffset, ut.SaeaBufSize)
            ut.okContUnit ()
        else
            ut.Continuation <- ProcessSend
            SetupSend saea ut
            //TODO "ensure SetupSent+ProcessSend have not called uk.okCont already"



let Reset (saea:SocketAsyncEventArgs)=
    let ut = saea.UserToken :?> UserToken    
    ut.SaeaBufEnd <- 0
    ut.SaeaBufStart <- 0
    ut.ClientBuf <- [||]
    ut.ClientBufPos <- 0
    ut.okContSingleByte <- ignore
    ut.okContBytes <- ignore
    ut.okContUnit <- ignore
    ut.exnCont <- ignore 
    ut.cancCont <- ignore 
    Array.Clear( saea.Buffer, ut.SaeaBufOffset, ut.SaeaBufSize )
    saea.SetBuffer(ut.SaeaBufOffset, ut.SaeaBufSize)
    ut.BufList.Clear()


let OnClientIOCompleted (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    ut.Continuation saea


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


