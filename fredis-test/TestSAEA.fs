module TestSAEA


//open Xunit
open FsCheck
open FsCheck.Xunit
//open Swensen.Unquote

open System.Net
open System.Net.Sockets
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading.Tasks
open SocAsyncEventArgFuncs
open System




//let host = "0.0.0.0"
let host = "127.0.0.1"
let port = 6379
let ipAddr = IPAddress.Parse(host)
let localEndPoint = IPEndPoint (ipAddr, port)



let private ToIList<'T> (bs : 'T array) =
    let segment = new System.ArraySegment<'T>(bs)
    let data = new List<System.ArraySegment<'T>>() :> IList<System.ArraySegment<'T>>
    data.Add(segment)
    data



// convenience extensions, for these tests only, taken from https://msdn.microsoft.com/en-us/library/ee370564.aspx
type private Socket with

    member this.MyAcceptAsync() =
        Async.FromBeginEnd((fun (callback, state) -> this.BeginAccept(callback, state)),
                            this.EndAccept  )
    
    member this.MyConnectAsync(ipAddress : IPAddress, port : int) =
        Async.FromBeginEnd( ipAddress, 
                            port,
                            (fun (ipAddress:IPAddress, port, callback, state) -> this.BeginConnect(ipAddress, port, callback, state)),
                            this.EndConnect )
    
    member this.MySendAsync(data : byte array ) =
        let flags = SocketFlags.None
        Async.FromBeginEnd( ToIList data, 
                            flags, 
                            (fun (data : IList<System.ArraySegment<byte>>, flags, callback, state) -> this.BeginSend(data, flags, callback, state)),
                            this.EndSend    )
    
    member this.MyReceiveAsync(data : byte array) =
        let flags = SocketFlags.None
        Async.FromBeginEnd( ToIList data, 
                            flags, 
                            (fun (data : IList<System.ArraySegment<byte>>, flags : SocketFlags, callback, state) -> this.BeginReceive(data, flags, callback, state)),
                            this.EndReceive )




let mutable (saeaPoolM:ConcurrentStack<SocketAsyncEventArgs>) = null

// calling CreateClientSAEAPool from inside test functions allows different values of numClient and saea buffer sizes
let CreateClientSAEAPool maxNumClients saeaBufSize = 
    let saeaSharedBuffer = Array.zeroCreate<byte> (maxNumClients * saeaBufSize)
    let saeaPool = new ConcurrentStack<SocketAsyncEventArgs>()
    for ctr = 0 to (maxNumClients - 1) do
        let saea   = new SocketAsyncEventArgs()
        let offset = ctr*saeaBufSize
        saea.SetBuffer(saeaSharedBuffer, offset, saeaBufSize)
        saea.add_Completed (fun _ b -> SocAsyncEventArgFuncs.OnClientIOCompleted b)
        let ut = {
            Socket = null
            Tcs = null
            ClientBuf = null
            ClientBufPos = -1
            SaeaBufStart = saeaBufSize
            SaeaBufEnd = saeaBufSize
            SaeaBufSize = saeaBufSize
            Continuation = ignore
            BufList = System.Collections.Generic.List<byte[]>()
            Expected = null
        }
        saea.UserToken <- ut
        saeaPool.Push(saea)
    saeaPool



let ProcessAccept (saeaAccept:SocketAsyncEventArgs) = 
    let clientSocket = saeaAccept.AcceptSocket
    match saeaPoolM.TryPop() with
    | true, clientSaea  -> 
            let clientUserToken = clientSaea.UserToken :?> UserToken
            clientUserToken.Socket <- clientSocket
            let tcs = saeaAccept.UserToken :?> TaskCompletionSource<SocketAsyncEventArgs>
            tcs.SetResult(clientSaea)
    | false, _  -> 
            failwith "failed to allocate saea"



let StartAccept (listenSocket:Socket) (acceptEventArg:SocketAsyncEventArgs) =
    let tcs = TaskCompletionSource<SocketAsyncEventArgs>()
    acceptEventArg.UserToken <- tcs
    let ioPending = listenSocket.AcceptAsync acceptEventArg
    if not ioPending then
        ProcessAccept acceptEventArg
    tcs.Task |> Async.AwaitTask



let rec private ArraySubSeqs (bs:byte[]): byte array seq = 
    let bs2 = Array.tail bs
    match bs2 with
    | [||]  -> Seq.empty
    | bs2   -> seq{ yield bs2; yield! ArraySubSeqs bs2 }



let genNonEmptyBytes = 
    gen{
        let! arraySize = Gen.choose (1, 1024)
        let! bs = Gen.arrayOfLength arraySize Arb.generate<byte>
        return bs
    }    


type ArbOverridesAsyncRead() =
    static member NonEmptyByteArray() = Arb.fromGenShrink (genNonEmptyBytes, ArraySubSeqs)

 
type SaeaAsyncReadPropertyAttribute() =
    inherit PropertyAttribute(
        Arbitrary = [| typeof<ArbOverridesAsyncRead> |],
        MaxTest = 1000,
        Verbose = false,
        QuietOnSuccess = false)



let myGenByteNoCRLF = Arb.generate<byte>  |> Gen.suchThat (fun bb -> bb <> 13uy && bb <> 10uy)

let genNonEmptyBytesNoCRLF = 
    gen{
        let! arraySize = Gen.choose (1, 1024 * 8)
        let! bs = Gen.arrayOfLength arraySize myGenByteNoCRLF
        return bs
    }

type ArbOverridesAsyncReadCRLF() =
    static member NonEmptyByteArray() = Arb.fromGenShrink (genNonEmptyBytesNoCRLF, ArraySubSeqs)


type SaeaAsyncReadCRLFPropertyAttribute() =
    inherit PropertyAttribute(
        Arbitrary = [| typeof<ArbOverridesAsyncReadCRLF> |],
        MaxTest = 1000,
        Verbose = false,
        QuietOnSuccess = false )


//let (.=.) left right = left = right |@ sprintf "\n%A =\n%A" left right


let SetupListenerSocket (maxNumClients:int) : Socket = 
    let listenSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
    listenSocket.Bind(localEndPoint)
    listenSocket.Listen maxNumClients
    listenSocket



[<SaeaAsyncReadCRLFPropertyAttribute>]
let ``saea AsyncReadUntilCRLF CRLF, previous read has populated the saea buffer`` (bsToSend1:byte[]) (bsToSend2:byte[])  =
    // arrange
    let maxNumClients = 8
    let clientBufSize = 1024 * 4

    saeaPoolM <- CreateClientSAEAPool maxNumClients clientBufSize
    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
    use listenSocket = SetupListenerSocket maxNumClients

    let bsToSendCRLF = seq{yield! bsToSend1; yield! bsToSend2; yield 13uy; yield 10uy} |> Seq.toArray // append CRLF to the bytes being send (which won't contain CRLF due to the custom Arb instance)


    // act 
    let asyncReceive = async{
        let! saea = StartAccept listenSocket acceptEventArg
        let ut = saea.UserToken :?> UserToken
        ut.Expected <- bsToSend2
        let! bs1 = SocAsyncEventArgFuncs.AsyncRead2 saea bsToSend1.Length // read the first set of bytes, then throw away
        assert (bs1 = bsToSend1)
        let! bs2 = SocAsyncEventArgFuncs.AsyncReadUntilCRLF saea
        return bs2
    }

    let asyncSend = async{
        use  clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do!  clientClientSocket.MyConnectAsync(ipAddr, port)
        let! _ = clientClientSocket.MySendAsync(bsToSendCRLF)
        return bsToSendCRLF
    } 
    
    let xx = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously

    // assert
    let ok, received = 
        match xx with
        |[|_; received|]    -> bsToSend2 = received, received
        | _                 -> false, [||]

    if not  ok then
        let m3 = sprintf "#### received    : %A" received
        System.Diagnostics.Trace.WriteLine m3

        let m4 = sprintf "#### expected    : %A" bsToSend2
        System.Diagnostics.Trace.WriteLine m4

        let msg = sprintf "#### failing array len: %d" bsToSendCRLF.Length
        System.Diagnostics.Trace.WriteLine msg
    ok



[<SaeaAsyncReadCRLFPropertyAttribute>]
let ``saea AsyncReadByte - correct byte received`` (firstBsToSend:byte[]) (byteIn:byte) (moreBsToSend:byte[]) =
    let maxNumClients = 4
    let clientBufSize = 1024 * 4

    saeaPoolM <- CreateClientSAEAPool maxNumClients clientBufSize
    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
    use listenSocket = SetupListenerSocket maxNumClients

    let bsToSendCRLF = seq{yield! firstBsToSend; yield byteIn; yield! moreBsToSend} |> Seq.toArray 

    // act 
    let asyncReceive = async{
        let! saea = StartAccept listenSocket acceptEventArg
        let! bs1Out = SocAsyncEventArgFuncs.AsyncRead2 saea firstBsToSend.Length // read the first set of bytes, then throw away
        assert(bs1Out = firstBsToSend)
        let! byteOut = SocAsyncEventArgFuncs.AsyncReadByte saea
        let! bs2Out = SocAsyncEventArgFuncs.AsyncRead2 saea moreBsToSend.Length // read the first set of bytes, then throw away
        assert (bs2Out = moreBsToSend)
        return [|byteOut|] // wrapping in an array to make asyncReceive the same type as asyncSend
    }

    let asyncSend = async{
        use  clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do!  clientClientSocket.MyConnectAsync(ipAddr, port)
        let! _ = clientClientSocket.MySendAsync(bsToSendCRLF)
        return bsToSendCRLF
    } 
    
    let asyncResults = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously

    // assert
    match asyncResults with
    |[|_; [|byteOut|]|] -> byteOut = byteIn
    | _                 -> false



[<SaeaAsyncReadCRLFPropertyAttribute>]
let ``saea AsyncReadUntilCRLF - CRLF received already in saea buffer from previous read`` (bs:byte[])  =
    let maxNumClients = 4
    let clientBufSize = bs.Length + 100 
    saeaPoolM <- CreateClientSAEAPool maxNumClients clientBufSize
    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
    use listenSocket = SetupListenerSocket maxNumClients
    let firstBsToSend = "some bytes"B
    let moreBytes = "more bytes"B
    let bsToSendCRLF = seq{yield! firstBsToSend; yield! bs; yield 13uy; yield 10uy; yield! moreBytes} |> Seq.toArray 

    // act 
    let asyncReceive = async{
        let! saea = StartAccept listenSocket acceptEventArg
        let ut = saea.UserToken :?> UserToken
        ut.Expected <- bs
        let! bsOut = SocAsyncEventArgFuncs.AsyncRead2 saea firstBsToSend.Length // read the first set of bytes, then throw away
        assert (bsOut = firstBsToSend)
        let! bsOut2 = SocAsyncEventArgFuncs.AsyncReadUntilCRLF saea
        let! bsOut3 = SocAsyncEventArgFuncs.AsyncRead2 saea moreBytes.Length // read the first set of bytes, then throw away
        assert( moreBytes = bsOut3)
        return bsOut2
    }

    let asyncSend = async{
        use  clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do!  clientClientSocket.MyConnectAsync(ipAddr, port)
        let! _ = clientClientSocket.MySendAsync(bsToSendCRLF)
        return bsToSendCRLF
    } 
    
    let xx = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously

    // assert
    match xx with
    |[|_; received|]    -> bs = received
    | _                 -> false



[<SaeaAsyncReadPropertyAttribute>]
let ``saea AsyncRead single send-receive property test`` (bsToSend:byte[]) =
    // arrange
    let maxNumClients = 16
    let clientBufSize = 16
    saeaPoolM <- CreateClientSAEAPool maxNumClients clientBufSize
    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
    use listenSocket = SetupListenerSocket maxNumClients

    // act 
    let asyncReceive = async{
        let! saea = StartAccept listenSocket acceptEventArg
        return! SocAsyncEventArgFuncs.AsyncRead2 saea bsToSend.Length
    }

    let asyncSend = async{
        use  clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do!  clientClientSocket.MyConnectAsync(ipAddr, port)
        let! _ = clientClientSocket.MySendAsync(bsToSend)
        return bsToSend // asyncSend needs to be of the same type as asyncReceive to run in parallel
    } 
    
    let xx = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously

    // assert
    match xx with
    |[|sent; received|] -> sent = received
    | _                 -> false



// test that reading for bsToSend1 followed by a read for bsToSend2.Length gives bsToSend2 for the second read
[<SaeaAsyncReadPropertyAttribute>]
let ``saea AsyncWrite property test`` (bsToSend1:byte[]) =
    // arrange
    let maxNumClients = 16
    let clientBufSize = 256
    saeaPoolM <- (CreateClientSAEAPool maxNumClients clientBufSize)
    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
    use listenSocket = SetupListenerSocket maxNumClients

    // act
    let asyncSend = async{
        let! saea = StartAccept listenSocket acceptEventArg
        do! SocAsyncEventArgFuncs.AsyncWrite saea bsToSend1
        return [||]
    }

    let asyncReceive = async{
        use clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do! clientClientSocket.MyConnectAsync(ipAddr, port)
        match saeaPoolM.TryPop() with
        | true, clientSaea  -> 
            let ut = clientSaea.UserToken :?> UserToken
            ut.Socket <- clientClientSocket
            let! bsReceived = (SocAsyncEventArgFuncs.AsyncRead2 clientSaea bsToSend1.Length)
            return bsReceived // asyncSend needs to be of the same type as asyncReceive to run in parallel
        | false, _  ->
            failwith "failed to allocate saea in \"saea AsyncWrite property test\""
            return [||]
    } 

    // assert    
    let xs = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously 
    match xs with
    |[|_; bsReceived|]  ->  bsReceived = bsToSend1
    | _                 ->  false




// test that reading for bsToSend1 followed by a read for bsToSend2.Length gives bsToSend2 for the second read
[<SaeaAsyncReadPropertyAttribute>]
let ``saea AsyncRead x1 send x3 receive property test`` (bsToSend1:byte[]) (bsToSend2:byte[]) (bsToSend3:byte[]) =

    // arrange
    let maxNumClients = 16
    let clientBufSize = 16
    saeaPoolM <- (CreateClientSAEAPool maxNumClients clientBufSize)
    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
    use listenSocket = SetupListenerSocket maxNumClients

    // act
    let bsToSendAll = seq{ yield! bsToSend1; yield! bsToSend2; yield! bsToSend3 } |> Seq.toArray 

    let asyncReceive = async{
        let! saea = StartAccept listenSocket acceptEventArg
        let! b1 = SocAsyncEventArgFuncs.AsyncRead2 saea bsToSend1.Length
        let! b2 = SocAsyncEventArgFuncs.AsyncRead2 saea bsToSend2.Length
        let! b3 = SocAsyncEventArgFuncs.AsyncRead2 saea bsToSend3.Length
        return [|b1;b2;b3|]; 
    }

    let asyncSend = async{
        use  clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do!  clientClientSocket.MyConnectAsync(ipAddr, port)
        let! _ = clientClientSocket.MySendAsync(bsToSendAll)
        return [|bsToSendAll|] // asyncSend needs to be of the same type as asyncReceive to run in parallel
    } 
    
    let xss = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously 

    // assert
    // flatten the returned array of arrays by one level
    let xs = [| for xs in xss do
                for x in xs do               
                yield x |]

    match xs with
    |[|_; b1; b2; b3|]  ->  bsToSend1 = b1 &&
                            bsToSend2 = b2 && 
                            bsToSend3 = b3
    | _                 ->  false




[<SaeaAsyncReadPropertyAttribute>]
let ``saea AsyncRead x3 send x1 receive property test``  (bsToSend1:byte[]) (bsToSend2:byte[]) (bsToSend3:byte[]) =

    // arrange
    let maxNumClients = 16
    let clientBufSize = 16
    saeaPoolM <- (CreateClientSAEAPool maxNumClients clientBufSize)
    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
    use listenSocket = SetupListenerSocket maxNumClients
    let bsToSendAll = seq{ yield! bsToSend1; yield! bsToSend2; yield! bsToSend3 } |> Seq.toArray 

    // act
    let asyncReceive = async{
        let! saea = StartAccept listenSocket acceptEventArg
        let! bsRead = SocAsyncEventArgFuncs.AsyncRead2 saea bsToSendAll.Length
        return [|bsRead|]; 
    }

    let asyncSend = async{
        use  clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do!  clientClientSocket.MyConnectAsync(ipAddr, port)
        let! _ = clientClientSocket.MySendAsync(bsToSend1)
        let! _ = clientClientSocket.MySendAsync(bsToSend2)
        let! _ = clientClientSocket.MySendAsync(bsToSend3)
        return [|bsToSend1; bsToSend2; bsToSend3 |] // asyncSend needs to be of the same type as asyncReceive to run in parallel
    } 
    
    let xss = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously 

    // assert
    // flatten the returned array of arrays by one level
    let xs = [| for xs in xss do
                for x in xs do               
                yield x |]

    match xs with
    |[|_; _; _; bsReceived|]    ->  bsToSendAll = bsReceived
    | _                         ->  false




