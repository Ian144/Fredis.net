module TestRespMsgParser



open Xunit
open Swensen.Unquote


// https://code.google.com/p/fsunit/


[<Fact>]
let ``ReadUntilCRLF basic`` () =
    use strm = new System.IO.MemoryStream()
    let bs = Utils.StrToBytes "678\r\n"
    strm.Write( bs, 0, bs.Length)
    let _ = strm.Seek(0L, System.IO.SeekOrigin.Begin)
    test <@ [54;55;56] = RespMsgProcessor.ReadUntilCRLF( strm ) @>



[<Fact>]
let ``ReadUntilCRLF not empty after CRLF`` () =
    use strm = new System.IO.MemoryStream()
    let bs = Utils.StrToBytes "678\r\n123"
    strm.Write( bs, 0, bs.Length)
    let _ = strm.Seek(0L, System.IO.SeekOrigin.Begin)
    test <@ [54;55;56] = RespMsgProcessor.ReadUntilCRLF( strm ) @>



[<Fact>]
let ``ReadUntilCRLF at end of stream returns empty list`` () =
    use strm = new System.IO.MemoryStream()
    test <@ [] = RespMsgProcessor.ReadUntilCRLF( strm ) @>
    


[<Fact>]
let ``ReadInt32 basic`` () =
    use strm = new System.IO.MemoryStream()
    let bs = Utils.StrToBytes "678\r\n"
    do  strm.Write( bs, 0, bs.Length)
    let _ = strm.Seek(0L, System.IO.SeekOrigin.Begin)
    test <@ 678 = RespMsgProcessor.ReadInt32( strm ) @> 



[<Fact>]
let ``ReadInt32 at end of stream returns zero`` () =
    use strm = new System.IO.MemoryStream()
    test <@ 0 = RespMsgProcessor.ReadInt32( strm ) @>




