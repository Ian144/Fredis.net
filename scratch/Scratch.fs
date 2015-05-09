module Scratch



// MSET key1 "Hello" key2 "World"
// *5
// $4
// MSET
// $4
// key1
// $5
// Hello
// $4
// key2
// $5
// World




//let counter =
//    MailboxProcessor.Start(
//        fun (inbox:MailboxProcessor<RedisCmd list>) ->
//            while (
//            let rec loop str =
//                async { let! cmds = inbox.Receive()
//                        return! loop(n+msg) }
//            loop ()
  
// 
//let xx = System.Threading.ThreadPool.SetMaxThreads(1,1)
//printfn "xx"


//    let asyncProcessPing =
//        async{
//            let ns = tc.GetStream()
//            while tc.Connected do
//                let! respTypeByte    = ns.AsyncReadByte()
//                let! respTypeByte2   = ns.AsyncReadByte()
//                let! respTypeByte3   = ns.AsyncReadByte()
//                let! respTypeByte4   = ns.AsyncReadByte()
//                let! respTypeByte5   = ns.AsyncReadByte()
//                let! respTypeByte6   = ns.AsyncReadByte()
//                do! (ns.AsyncWrite pongBytes)
//
//            tc.Close()
//        }
//
//    let asyncSyncProcessPing =
//        async{
//            let ns = tc.GetStream()
//            while tc.Connected do
//                let! respTypeByte   = ns.AsyncReadByte()     // synchronous after this point, until current cmd(s) processed
//                Eat5Bytes ns
//                do! (ns.AsyncWrite pongBytes)
//                ns.Write( pongBytes,0,7) |> ignore
//            tc.Close()
//        }
//    
//    let asyncProcessPing2 =
//        async{
//            let sz = 7
//            let bs6 = Array.zeroCreate<byte> sz 
//            let ns = tc.GetStream()
//            while tc.Connected do
//                let tsk = ns.ReadAsync(bs6,0,sz)
//                let! _ = Async.AwaitTask tsk
//                do! (ns.AsyncWrite pongBytes)
//            tc.Close()
//        }

//    let asyncDumpStreamIn =
//        let ns = tc.GetStream()
//        async{
//            while true do
//                let! b = ns.AsyncReadByte ()
//                do printf "%A" b
//        }
//
//    let asyncDumpStreamInStr  =
//        let ns = tc.GetStream()
//        async{
//            while true do
//                let! str = ns.AsyncReadString
//                do printf "%s" str
//        }



let setBitpos (bytes:byte []) =
    let ba = System.Collections.BitArray(bytes)
    let arr = Array.zeroCreate<bool>(ba.Length)
    
    for idx = 0 to ba.Length - 1 do
        arr.[idx] <- ba.Item(idx)

    match arr |> Array.exists (fun bl -> bl) with
    | true -> arr |> Array.findIndex (fun bl -> bl)
    | false -> -1


let bs = [|0uy..255uy|]

let bas = [|    for b in bs do
                yield [|b|] |]

bas |> Array.iter (fun ba -> printf "%d; " (setBitpos ba) )  