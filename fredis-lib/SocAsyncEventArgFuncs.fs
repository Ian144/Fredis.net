module SocAsyncEventArgFuncs


open System
open System.Net.Sockets
open System.Threading.Tasks









[<NoEquality;NoComparison>]
type UserToken = {
    mutable Socket: Socket
    mutable Tcs: TaskCompletionSource<byte[]>
    mutable ClientBuf: byte[]   // consider a struct holding both ClientBuf and ClientBufPos
    mutable ClientBufPos: int
    mutable SaeaBufStart: int   // offsets into each saea's section of the shared buffer, does not take into account the offset of the shared section
    mutable SaeaBufEnd: int
    SaeaBufSize: int    // the size of that part of the  shared buffer available to each saea
    SaeaBufOffset:int
    mutable Continuation: SocketAsyncEventArgs -> unit
    BufList: System.Collections.Generic.List<byte[]> // used when loading an unknown number of bytes, such as when reading up to a delimiter
    mutable Expected: byte[]
    }


[<NoEquality;NoComparison>]
type IFredisStreamSource =
    abstract AsyncReadUntilCRLF : unit -> Async<byte[]>
    abstract AsyncReadNBytes : int -> Async<byte[]>
    abstract AsyncReadByte : unit -> Async<byte>
    abstract AsyncEatCRLF: unit -> Async<unit> // could be just a convenience wrapper arount AsyncReadN, but sometimes might be able to avoid array alloc and copying 


[<NoEquality;NoComparison>]
type IFredisStreamSink =
    abstract AsyncWrite : byte[] -> Async<unit>
    abstract AsyncWriteWithCRLF : byte[] -> Async<unit>
//    abstract AsyncWriteBuf : byte[] -> Async<unit>
    abstract AsyncFlush : unit -> Async<unit>




let rec ProcessReceive (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    let bytesTransferred = saea.BytesTransferred
    let bytesRequired = ut.ClientBuf.Length - ut.ClientBufPos

    match saea.SocketError, bytesTransferred, bytesRequired with
    | SocketError.Success, tran, req when req = tran ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset, ut.ClientBuf, ut.ClientBufPos, req)
            ut.ClientBufPos <- ut.ClientBuf.Length
            ut.SaeaBufStart <- bytesRequired    // could be read in subsequent calls
            ut.SaeaBufEnd   <- bytesTransferred
            ut.Tcs.SetResult(ut.ClientBuf)
    | SocketError.Success, tran, req when req < tran ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset, ut.ClientBuf, ut.ClientBufPos, req)
            ut.ClientBufPos <- ut.ClientBuf.Length
            ut.SaeaBufStart <- req
            ut.SaeaBufEnd   <- tran
            ut.Tcs.SetResult(ut.ClientBuf)
    | SocketError.Success, tran, req when req > tran ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset, ut.ClientBuf, ut.ClientBufPos, bytesTransferred)
            ut.ClientBufPos <- ut.ClientBufPos + bytesTransferred
            ut.SaeaBufStart <- ut.SaeaBufSize
            ut.SaeaBufEnd   <- ut.SaeaBufSize
            let ioPending = ut.Socket.ReceiveAsync saea
            if not ioPending then
                ProcessReceive(saea)
    | SocketError.Success, 0, _ -> 
            ut.Tcs.SetCanceled()    // client has disconnected
    | err, _, _ ->
            let msg = sprintf "receive socket error: %O" err
            let ex = new Exception(msg)
            ut.Tcs.SetException(ex)


             
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


let private flattenListBuf listBufs = 
    // todo: this may be slow, possibly involving multiple allocations
    [| for b in listBufs do yield! b |]
    

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
             // client has disconnected
            ut.BufList.Clear()
            ut.Tcs.SetCanceled()   
    | SocketError.Success, true, crIdx,_  when crIdx < (endIdx - 1) ->
            ut.ClientBufPos <- ut.ClientBuf.Length
            ut.SaeaBufStart <- crIdx + 2 - saea.Offset  // +2 to ingore the CRLF
            ut.SaeaBufEnd   <- endIdx - saea.Offset
            let len = crIdx - startIdx
            let buf = Array.zeroCreate<byte> len
            Buffer.BlockCopy(saea.Buffer, saea.Offset, buf, 0, len)
            ut.BufList.Add buf 
            let bufAll = flattenListBuf ut.BufList
            let ok = ut.Expected = bufAll // todo: remove UserToken.Expected, currently used for debugging failed tests
            ut.BufList.Clear()
            ut.Tcs.SetResult(bufAll)
    | SocketError.Success, true, crIdx, _  when crIdx = (endIdx - 1) ->
            ut.Continuation <- ProcessReceiveUntilCRLFEatFirstByte
            let len = crIdx - ut.SaeaBufStart
            let buf = Array.zeroCreate<byte> len
            Buffer.BlockCopy( saea.Buffer, startIdx, buf, 0, len)
            ut.BufList.Add buf
            ut.SaeaBufStart <- ut.SaeaBufSize// there is still the CR in the last element to read
            ut.SaeaBufEnd <- ut.SaeaBufSize
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessReceiveUntilCRLFEatFirstByte(saea)
    | SocketError.Success, true, crIdx, _  when crIdx > (endIdx - 1) ->
            failwith "invalid carriage return index found"
    | SocketError.Success, false, _, _  ->
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
            let msg = sprintf "receive socket error: %O" err
            let ex = new Exception(msg)
            ut.BufList.Clear()
            ut.Tcs.SetException(ex)

// called when a CR has been read as the last byte of a previous read
// assuming the first byte is LF, todo: dont assume
and ProcessReceiveUntilCRLFEatFirstByte (saea:SocketAsyncEventArgs) =
    let ut = saea.UserToken :?> UserToken
    let bytesTransferred = saea.BytesTransferred
    ut.SaeaBufStart <- 0
    ut.SaeaBufEnd <- bytesTransferred

    match saea.SocketError, bytesTransferred with
    | SocketError.Success, 0 -> 
            ut.BufList.Clear()
            ut.Tcs.SetCanceled()    // client has disconnected

    | SocketError.Success, bytesTransferred   ->
            // eat the first char and collate the result
            ut.SaeaBufStart <- 1   
            let bufArr  = ut.BufList |> Seq.toArray                
            // todo: this may be slow, possibly involving multiple allocations
            let buf = [| for b in bufArr do
                         yield! b |]
            ut.BufList.Clear()
            let ok = ut.Expected = buf
            ut.Tcs.SetResult(buf)

    | err, _ ->
            ut.BufList.Clear()
            let msg = sprintf "receive socket error: %O" err
            let ex = new Exception(msg)
            ut.Tcs.SetException(ex)



// requires a buffer to be supplied, enabling pre-allocated buffers to be reused
let AsyncRead (saea:SocketAsyncEventArgs) (dest:byte []) : Async<byte[]> =
    let ut = saea.UserToken :?> UserToken
    ut.ClientBuf <- dest
    ut.ClientBufPos <- 0   // AsyncRead will always read into the client Buffer starting at index zero
    ut.Continuation <- ProcessReceive
    let availableBytes = ut.SaeaBufEnd - ut.SaeaBufStart // some bytes may have been read-in already, by a previous read operation
    match availableBytes with
    | _ when availableBytes >= dest.Length ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset + ut.SaeaBufStart, dest, 0, dest.Length) // able to satisfy the entire read request from bytes already available
            ut.SaeaBufStart <- ut.SaeaBufStart + dest.Length // mark the new begining of the unread bytes
            async{ return dest} //todo: consider could f# async be replaced with an SAEA style 'return bool to indicate if action was async or not' 
    | _ when availableBytes > 0 ->
            Buffer.BlockCopy(saea.Buffer, saea.Offset + ut.SaeaBufStart, dest, 0, availableBytes)
            ut.ClientBufPos <- availableBytes // availableBytes have been written into the client array (aka 'dest'), so the first unread index is 'availableBytes' because dest is zero based
            let tcs = new TaskCompletionSource<byte[]>()
            ut.Tcs <- tcs
            ut.SaeaBufStart <- ut.SaeaBufSize
            ut.SaeaBufEnd   <- ut.SaeaBufSize
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessReceive(saea)
            tcs.Task  |> Async.AwaitTask
    | _ ->  let tcs = new TaskCompletionSource<byte[]>()
            ut.Tcs <- tcs
            ut.SaeaBufStart <- ut.SaeaBufSize
            ut.SaeaBufEnd   <- ut.SaeaBufSize
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessReceive(saea)
            tcs.Task |> Async.AwaitTask



// does not require a buffer to be supplied
let AsyncRead2 (saea:SocketAsyncEventArgs) (len:int) : Async<byte[]> =
    let dest = Array.zeroCreate len // todo, consider how to avoid an alloc in AsyncRead2
    AsyncRead saea dest


// requires a buffer to be supplied, enabling pre-allocated buffers to be reused
let AsyncReadUntilCRLF (saea:SocketAsyncEventArgs) : Async<byte[]> =
    let ut = saea.UserToken :?> UserToken
    ut.Continuation <- ProcessReceiveUntilCRLF
    let endIdx = ut.SaeaBufEnd + saea.Offset
    let startIdx = ut.SaeaBufStart + saea.Offset
    let crFound, crIdx = FindCR saea.Buffer startIdx endIdx
    // todo: check that LF follows CR

    match crFound, crIdx  with
    | true, crIdx when crIdx < (endIdx - 1)   ->   
            // CR found and there is at least one more char to read without doing another receive
            // hopefully this is the common case, as others are potentially slow
            let len = (crIdx - startIdx) // -1 as the CR is being ignored
            let arrOut = Array.zeroCreate len
            Buffer.BlockCopy( saea.Buffer, startIdx, arrOut,0, len)
            ut.SaeaBufStart <- ut.SaeaBufStart + len + 2 // +2 so as to go past the CRLF
            //no change to ut.SaeaBufEnd should be made, it still marks the end of valid bytes in the saea buffer
            async{ return arrOut }
    | true, _   ->  
            // the CR is the last char in the saea buffer, so read again, and throw away the first byte
            ut.Continuation <- ProcessReceiveUntilCRLFEatFirstByte
            let len = (ut.SaeaBufEnd - ut.SaeaBufStart) - 1 // -1 to ignore the CR at the end
            assert (len > 0)
            let buf1 = Array.zeroCreate<byte> len
            Buffer.BlockCopy( saea.Buffer, startIdx, buf1, 0, len)
            ut.BufList.Add buf1
            ut.SaeaBufStart <- ut.SaeaBufSize // the entire saea buffer has now been read into the client buffer, so set the saea buf to empty
            ut.SaeaBufEnd <- ut.SaeaBufSize
            ut.Tcs <- TaskCompletionSource<byte[]>()
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessReceiveUntilCRLFEatFirstByte(saea)
            ut.Tcs.Task |> Async.AwaitTask
    | false, _   ->  
            // CRLF not found, continue reading until it is
            let len = ut.SaeaBufEnd - ut.SaeaBufStart
            if len > 0 then
                let buf1 = Array.zeroCreate<byte> len
                Buffer.BlockCopy( saea.Buffer, startIdx, buf1, 0, len)
                ut.BufList.Add buf1
            ut.SaeaBufStart <- ut.SaeaBufSize // the entire saea buffer has now been read into the client buffer, so set the saea buf to empty
            ut.SaeaBufEnd <- ut.SaeaBufSize
            ut.Tcs <- TaskCompletionSource<byte[]>()
            let ioPending = ut.Socket.ReceiveAsync(saea)
            if not ioPending then
                ProcessReceiveUntilCRLF(saea)
            ut.Tcs.Task |> Async.AwaitTask            


// avoids array alloc and copying when that data is already available
let AsyncReadByte (saea:SocketAsyncEventArgs) : Async<byte> =
    let ut = saea.UserToken :?> UserToken

    let availableBytes = ut.SaeaBufEnd - ut.SaeaBufStart // some bytes may have been read-in already, by a previous read operation

    match availableBytes with
    | _ when availableBytes > 0 ->
            let bb = saea.Buffer.[saea.Offset + ut.SaeaBufStart]
            ut.SaeaBufStart <- ut.SaeaBufStart + 1 // mark the new begining of the unread bytes
            async{ return bb }
    
    | _ ->  // the hopefully rare case where asking for a single byte requires another socket read
            async{
                let! bs = AsyncRead2 saea 1
                return bs.[0]
            }




let AsyncEatCRLF (saea:SocketAsyncEventArgs) : Async<unit> =
    let ut = saea.UserToken :?> UserToken

    let availableBytes = ut.SaeaBufEnd - ut.SaeaBufStart // some bytes may have been read-in already, by a previous read operation

    match availableBytes with
    | _ when availableBytes > 1 ->  // hopefully the common case, where asking for 2 bytes does not require array allocation
            let crPos = saea.Offset + ut.SaeaBufStart
            let lfPos = crPos + 1
            if not (saea.Buffer.[crPos] = 13uy && saea.Buffer.[lfPos] = 10uy ) then failwith "AsyncEatCRLF next bytes are not CRLF"
            ut.SaeaBufStart <- ut.SaeaBufStart + 2 // mark the new begining of the unread bytes
            async{return ()}
    
    | _ ->  // the hopefully rare case where asking for a 2 byes requires another socket read
            async{
                let! bs = AsyncRead2 saea 2 // can i avoid the alloc here? maybe does not happen often so does it matter?
                if bs <>  [|13uy;10uy|] then failwith "AsyncEatCRLF next bytes are not CRLF"
                return ()
            }



let rec private SetupSend (saea:SocketAsyncEventArgs) ut =
    let arr  = Array.zeroCreate<byte> (ut.SaeaBufEnd - ut.SaeaBufStart) 
    Buffer.BlockCopy(saea.Buffer, saea.Offset + ut.SaeaBufStart, arr, 0, ut.SaeaBufEnd - ut.SaeaBufStart )
    let str = arr |> Utils.BytesToStr
    let str2 = str.Replace("\r\n", "\\r\\n")
    //printfn "SetupSend: %s" str2
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
            match clientBufLenRemaining > 0, clientBufLenRemaining < ut.SaeaBufSize with
            | false, _      ->  
                //printfn "ProcessSend 1"
                ut.Tcs.SetResult([||])
            | true,  true  -> // there is enough space in the saeaBuffer to send the unsent part of the client buffer
                //printfn "ProcessSend 2"
                Buffer.BlockCopy(ut.ClientBuf, ut.ClientBufPos, saea.Buffer, saea.Offset, clientBufLenRemaining)                        
                ut.SaeaBufStart <- 0
                ut.SaeaBufEnd <- clientBufLenRemaining
                ut.ClientBufPos <- ut.ClientBufPos + clientBufLenRemaining
                assert (ut.ClientBufPos = ut.ClientBuf.Length)
                SetupSend saea ut
            | true,  false   ->  // there is not enough space in the saeaBuffer to send all of the unsent clientBuf
                //printfn "ProcessSend 3"
                Buffer.BlockCopy(ut.ClientBuf, ut.ClientBufPos, saea.Buffer, saea.Offset, ut.SaeaBufSize)                        
                ut.SaeaBufStart <- 0
                ut.SaeaBufEnd <- ut.SaeaBufSize
                ut.ClientBufPos <- ut.ClientBufPos +  ut.SaeaBufSize
                SetupSend saea ut
    | SocketError.Success, saeaBufLenRemaining when saeaBufLenRemaining > 0 -> 
            // not all of the bytes in the saea buffer were sent
            // send them, atm not shifting the unsent bytes to the begining of the saea buf and copying client bytes to the now available space
            //printfn "ProcessSend 4"
            SetupSend saea ut
    | err   ->
            let msg = sprintf "send socket error: %O" err
            let ex = new Exception(msg)
            ut.Tcs.SetException(ex)

let AsyncWrite (saea:SocketAsyncEventArgs) (bs:byte[]) : Async<unit> =
    let ut = saea.UserToken :?> UserToken
    let saeaBufSpaceAvailable = ut.SaeaBufSize - ut.SaeaBufEnd
    match bs.Length <= saeaBufSpaceAvailable with
    | true  ->
            // there is enough space available in the saea buf to store the contents of the client buf, which will be sent later
            //printfn "AsyncWrite enough space to store "
            Buffer.BlockCopy(bs, 0, saea.Buffer, saea.Offset + ut.SaeaBufEnd, bs.Length)
            ut.SaeaBufEnd <- ut.SaeaBufEnd + bs.Length
            ut.ClientBuf <- bs
            ut.ClientBufPos <- bs.Length // all bytes in bs have been copied to the saea buffwer
            async{ return() }
    | false ->
            // there is not enough space in the saea buf to accomodate bs
            //printfn "AsyncWrite NOT enough space to store "
            ut.ClientBuf <- bs
            ut.Continuation <- ProcessSend
            let tcs = new TaskCompletionSource<byte[]>()
            ut.Tcs <- tcs
            Buffer.BlockCopy(bs, 0, saea.Buffer, saea.Offset + ut.SaeaBufEnd, saeaBufSpaceAvailable)
            ut.SaeaBufEnd <- ut.SaeaBufSize // 
            ut.ClientBufPos <- saeaBufSpaceAvailable  // no part of bs will have been sent before this call to AsyncWrite
            SetupSend saea ut
            tcs.Task :> Task  |> Async.AwaitTask  //todo: converting a Task<byte[]> to a non-generic Task, fix this



// sends any unsent bytes in the saea buffer
let AsyncFlush (saea:SocketAsyncEventArgs) : Async<unit> =
    //printfn "AsyncFlush"
    let ut = saea.UserToken :?> UserToken    
    let numToSend = ut.SaeaBufEnd - ut.SaeaBufStart
    match numToSend with
    | 0 ->  async{ return() }
    | _ ->  ut.Continuation <- ProcessSend
            let tcs = new TaskCompletionSource<byte[]>()
            ut.Tcs <- tcs
            SetupSend saea ut
            tcs.Task :> Task |> Async.AwaitTask   //todo: converting a Task<byte[]> to a non-generic Task, fix this



let Reset (saea:SocketAsyncEventArgs)=
    let ut = saea.UserToken :?> UserToken    
    ut.SaeaBufEnd <- 0
    ut.SaeaBufStart <- 0
    ut.ClientBuf <- [||]
    ut.ClientBufPos <- 0
    Array.Clear( saea.Buffer, ut.SaeaBufOffset, ut.SaeaBufSize)
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
        member this.AsyncWriteWithCRLF _ = failwith "AsyncWriteWithCRLF not implemented"
        member this.AsyncFlush () = AsyncFlush saea


