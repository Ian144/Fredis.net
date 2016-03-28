module TestSAEA


open Xunit
open FsCheck
open FsCheck.Xunit
open Swensen.Unquote


open System.Net
open System.Net.Sockets
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading.Tasks
open SocAsyncEventArgFuncs




//let host = "0.0.0.0"
let host = "127.0.0.1"
let port = 6379


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
    
    member this.MyReceiveAsync(data : byte array, flags : SocketFlags) =
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
    
    | false, _          -> 
                failwith "failed to allocate saea"



let StartAccept (listenSocket:Socket) (acceptEventArg:SocketAsyncEventArgs) =
    let tcs = TaskCompletionSource<SocketAsyncEventArgs>()
    acceptEventArg.UserToken <- tcs
    let ioPending = listenSocket.AcceptAsync acceptEventArg
    if not ioPending then
        ProcessAccept acceptEventArg
    tcs.Task |> Async.AwaitTask




let genNonEmptyBytes = 
    gen{
        let! arraySize = Gen.choose (1, 128)
        let! bs = Gen.arrayOfLength arraySize Arb.generate<byte>
        return bs
    }



let private shrinkByteArray (bs:byte[]): byte array seq =
    let bs2 = Array.tail bs
    match bs2 with
    |[||]   -> Seq.empty
    |_      -> seq{yield bs2}
    


type ArbOverrides() =
    static member NonEmptyByteArray() = Arb.fromGenShrink (genNonEmptyBytes, shrinkByteArray)




[<Property( Arbitrary = [| typeof<ArbOverrides> |])>]
let saea (bsToSend:byte[])  =
    
    let msg = sprintf "bsToSend.Length %d" bsToSend.Length
    System.Diagnostics.Debug.WriteLine(msg )

    let maxNumClients = 16
    let individSaeaBufSize = 16
    
    saeaPoolM <- (CreateClientSAEAPool maxNumClients individSaeaBufSize)

    let ipAddr = IPAddress.Parse(host)
    let localEndPoint = IPEndPoint (ipAddr, port)
    use listenSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
    listenSocket.Bind(localEndPoint)
    listenSocket.Listen maxNumClients

    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)

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

    // CLEANUP CONNECTIONS, USE use

    match xx with
    |[|sent; received|] -> sent = received
    | _                 -> false



// do this with three reads
// compare all values readd
// smaller and larger that the buffer sizes
// test that reading for bsToSend1 followed by a read for bsToSend2.Length gives bsToSend2 for the second read
[<Property( Arbitrary = [| typeof<ArbOverrides> |])>]
let ``saea AsyncRead x1 send x3 receive property test`` (bsToSend1:byte[]) (bsToSend2:byte[]) (bsToSend3:byte[]) =

    let bsToSendAll = seq{ yield! bsToSend1; yield! bsToSend2; yield! bsToSend3 } |> Seq.toArray 

    let msg = sprintf "bsToSendAll.Length %d" bsToSendAll.Length
    System.Diagnostics.Debug.WriteLine(msg )

    let maxNumClients = 16
    let individSaeaBufSize = 16
    
    saeaPoolM <- (CreateClientSAEAPool maxNumClients individSaeaBufSize)

    let ipAddr = IPAddress.Parse(host)
    let localEndPoint = IPEndPoint (ipAddr, port)
    use listenSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
    listenSocket.Bind(localEndPoint)
    listenSocket.Listen maxNumClients

    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)

    let asyncReceive = async{
        let! saea = StartAccept listenSocket acceptEventArg
        let! b1 = SocAsyncEventArgFuncs.AsyncRead2 saea bsToSend1.Length
        let! b2 = SocAsyncEventArgFuncs.AsyncRead2 saea bsToSend2.Length
        let! b3 = SocAsyncEventArgFuncs.AsyncRead2 saea bsToSend2.Length
        return [|b1;b2;b3|]; 
    }

    let asyncSend = async{
        use  clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do!  clientClientSocket.MyConnectAsync(ipAddr, port)
        let! _ = clientClientSocket.MySendAsync(bsToSendAll)
        return [|bsToSendAll|] // asyncSend needs to be of the same type as asyncReceive to run in parallel
    } 
    
    let xss = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously 

    // flatten the returned array of arrays by one level
    let yy = [| for xs in xss do
                for x in xs do               
                yield x |]

    match yy with
    |[|_; b1; b2; b3|]  -> bsToSend1 = b1 && bsToSend2 = b2 && bsToSend3 = b3
    | _                 -> false




[<Fact>]
let ``server AsyncRead 14 char send, 2x7char reads `` () =

    let maxNumClients = 16
    let individSaeaBufSize = 1024
    saeaPoolM <- (CreateClientSAEAPool maxNumClients individSaeaBufSize)

    let ipAddr = IPAddress.Parse(host)
    let localEndPoint = IPEndPoint (ipAddr, port)
    use listenSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
    listenSocket.Bind(localEndPoint)
    listenSocket.Listen maxNumClients

    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)

    let bsToSend:byte[] = "abcdefgabcdefg"B

    let asyncReceive = async{
        let! saea = StartAccept listenSocket acceptEventArg
        let! _ = SocAsyncEventArgFuncs.AsyncRead2 saea 7
        return! SocAsyncEventArgFuncs.AsyncRead2 saea 7
    }
    

    let asyncSend = async{
        use  clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do!  clientClientSocket.MyConnectAsync(ipAddr, port)
        let! _ = clientClientSocket.MySendAsync(bsToSend)
        return bsToSend // asyncSend needs to be of the same type as asyncReceive to run in parallel
    } 
 

    let xx = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously


    let expected = "abcdefg"B

    match xx with
    |[|_; received|]    -> test<@ expected = received @>
    | _                 -> test<@ false  @>




[<Fact>]
let ``server AsyncRead 2x7 char sends, 1x14 char read `` () =

    let maxNumClients = 16
    let individSaeaBufSize = 1024
    saeaPoolM <- (CreateClientSAEAPool maxNumClients individSaeaBufSize)

    let ipAddr = IPAddress.Parse(host)
    let localEndPoint = IPEndPoint (ipAddr, port)
    use listenSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
    listenSocket.Bind(localEndPoint)
    listenSocket.Listen maxNumClients

    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)

    let bsToSend:byte[] = "abcdefg"B

    let asyncReceive = async{
        let! saea = StartAccept listenSocket acceptEventArg
        do! Async.Sleep 200
        return! SocAsyncEventArgFuncs.AsyncRead2 saea 14
    }
    

    let asyncSend = async{
        use  clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do!  clientClientSocket.MyConnectAsync(ipAddr, port)
        let! _ = clientClientSocket.MySendAsync(bsToSend)
        let! _ = clientClientSocket.MySendAsync(bsToSend)
        return bsToSend // asyncSend needs to be of the same type as asyncReceive to run in parallel
    } 
 

    let xx = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously


    let expected = "abcdefgabcdefg"B

    match xx with
    |[|_; received|]    -> test <@ expected = received @>
    | _                 -> test <@ false @>
