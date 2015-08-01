//    let asyncProcessClientRequestsSimple =
//        //let mutable (loopAgain:bool) = true
//        let loopAgain = ref true
//
//        let pongBytes  = Utils.StrToBytes "+PONG\r\n"
//        let totalBytesRead = ref 0
//        let buffers = Array.create<byte[]> 100000 [||]
//
//        async{
//            use client = client // without this Dispose would not be called on client
//            use ns = client.GetStream() 
//            while (client.Connected && !loopAgain) do
//                let! optRespTypeByte = ns.AsyncReadByte2()  // reading from the socket is synchronous after this point, until current redis msg is processed
//                printfn "handle new command"
//                match optRespTypeByte with
//                | None              -> loopAgain := false
//                | Some firstByte    ->
//                                let ctr = ref 0
//                                while client.Available > 0 do
//                                    let availToRead = client.Available
//                                    let buffer = Array.zeroCreate<byte> availToRead
//                                    let numBytesRead = ns.Read(buffer,0,availToRead) 
//                                    let idx:int = !ctr
//                                    buffers.[idx] <- buffer    
//                                    totalBytesRead := !totalBytesRead + numBytesRead 
//                                    ctr := !ctr + 1
//                                    printfn "numBytesRead: %d" numBytesRead
//                                let allBytes:byte[] =  buffers |> Array.collect id
//                                let ss = Utils.BytesToStr allBytes
//                                let firstChar = System.Convert.ToChar firstByte
//                                printfn "read:\n%c%s" firstChar  ss
//                                printfn "total numBytesRead: %d" !totalBytesRead
//                do! (ns.AsyncWrite pongBytes)
//        }
