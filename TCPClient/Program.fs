module Client

open System.Net
open System.Text
open System.IO
open System.Net.Sockets



let host = """127.0.0.1"""
let port = 6379


let pingBytes = [|42uy; 49uy; 13uy; 10uy; 36uy; 52uy; 13uy; 10uy; 80uy; 73uy; 78uy; 71uy; 13uy; 10uy|]


let mutable (loopAgain:bool) = true


let ipAddr = IPAddress.Parse(host)

//let client = new TcpClient ()
//client.Connect(ipAddr,port)
//let stream = client.GetStream ()
//
//
//while loopAgain do
//    stream.Write(pingBytes, 0, pingBytes.Length )
//    printfn "msg sent, press any key"
//    let str = stream.ReadString
//    printfn "received from server: %s" str
//    let key = System.Console.ReadKey().KeyChar
//    let xx =  not (key.Equals('x'))
//    loopAgain <- xx


let FuncLoopAgain () = 
    let key = System.Console.ReadKey().KeyChar
    loopAgain <-not (key.Equals('x'))




//while loopAgain do
//    let client = new TcpClient ()
//    client.Connect(ipAddr,port)
////    let stream = client.GetStream ()
////    stream.Write(pingBytes, 0, pingBytes.Length )
//    printfn "connected, press any key"
//    FuncLoopAgain()
//    client.Close()
//    printfn "connection closed, press any key"
//    FuncLoopAgain()


let client = new TcpClient ()
client.Connect(ipAddr,port)
printfn "connected, press any key"
FuncLoopAgain()
