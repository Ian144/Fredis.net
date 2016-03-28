module TestSAEA

open Xunit
open FsCheck
open FsCheck.Xunit
open Swensen.Unquote

open FredisTypes

open System.Net
open System.Net.Sockets
open System.Collections.Generic
open System.Threading.Tasks
open SocAsyncEventArgFuncs




//let host = "0.0.0.0"
let host = "127.0.0.1"
let port = 6379


(*
    create a server socket 
    create a client socket
    create a serverClient socket when client connects
    set bytes
    get bytes
    compare
    tear down
    
    
    xunit tests covering all states
    fscheck to hammer the thing
    
    test N clients with a shared buffer
    test send and receive from positions that don't start at zero bytes offset into the shared buffer
    refactor and cleanup once tests are in place

    listener.Accept blocks, so would never get to the client socket creation part
    options
        create a client and server thread
        async server and client

        f# async
        TPL
        BeginEnd

    Async.fromSAEA

*)



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



//[<NoEquality;NoComparison>]
//type UserToken = {
//    Tcs:TaskCompletionSource<SocketAsyncEventArgs> 
//}
    


let mutable serverReceived:byte array = [||]
let numClients = 1
let saeaBufSize = 1024 * 1024 * numClients
let saeaBuffer = Array.zeroCreate<byte> (saeaBufSize)
let clientSaea = new SocketAsyncEventArgs()
clientSaea.SetBuffer(saeaBuffer, 0, saeaBufSize )
clientSaea.add_Completed (fun _ b -> SocAsyncEventArgFuncs.OnClientIOCompleted b)

let ut = {
    Socket = null
    Tcs = null
    ClientBuf = null
    ClientBufPos = -1
    SaeaBufStart = saeaBufSize
    SaeaBufEnd = saeaBufSize
}

clientSaea.UserToken <- ut



let ProcessAccept (saeaAccept:SocketAsyncEventArgs) = 
    let tcs = saeaAccept.UserToken :?> TaskCompletionSource<SocketAsyncEventArgs>
    let clientSocket = saeaAccept.AcceptSocket
    let clientUt = {
        Socket = clientSocket
        Tcs = null
        ClientBuf = null
        ClientBufPos = -1
        SaeaBufStart = saeaBufSize
        SaeaBufEnd = saeaBufSize
    }

    clientSaea.UserToken <- clientUt
    tcs.SetResult(clientSaea)




let StartAccept (listenSocket:Socket) (acceptEventArg:SocketAsyncEventArgs) =
    let tcs = TaskCompletionSource<SocketAsyncEventArgs>()
    acceptEventArg.UserToken <- tcs
    let ioPending = listenSocket.AcceptAsync acceptEventArg
    if not ioPending then
        ProcessAccept acceptEventArg
    tcs.Task |> Async.AwaitTask



[<Fact>]
let ``server AsyncRead exp test`` () =

    let ipAddr = IPAddress.Parse(host)
    let localEndPoint = IPEndPoint (ipAddr, port)
    use listenSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
    listenSocket.Bind(localEndPoint)
    listenSocket.Listen numClients

    let acceptEventArg = new SocketAsyncEventArgs()
    acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)

    // cant let! on listenerSocket.clientConnected and 

    // do i need threads or a pair of asyncs

    let bsToSend:byte[] = "abcdefg"B

    let asyncReceive = async{
        let! saea = StartAccept listenSocket acceptEventArg
        return! SocAsyncEventArgFuncs.AsyncRead2 saea bsToSend.Length
    }
    

    let asyncSend = async{
        use  clientClientSocket = new Socket (localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        do!  clientClientSocket.MyConnectAsync(ipAddr, port)
        let! _ = clientClientSocket.MySendAsync(bsToSend) // put the receiving code after this?
        return bsToSend // asyncSend needs to be of the same type as asyncReceive
    } 
    
 
    
    let xx = [asyncSend;asyncReceive] |> Async.Parallel |> Async.RunSynchronously

    match xx with
    |[|sent; received|] -> test <@ sent = received @>
    | _                 -> test <@ false @>

