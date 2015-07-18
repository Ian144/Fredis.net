module TestRespMsgParser



open Xunit
open FsCheck
open FsCheck.Xunit
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
    


[<Property>]
let ``ReadInt32 basic`` (ii:int32) =
    use strm = new System.IO.MemoryStream()
    let bs = sprintf "%d\r\n" ii |> Utils.StrToBytes 
    do  strm.Write( bs, 0, bs.Length)
    let _ = strm.Seek(0L, System.IO.SeekOrigin.Begin)
    ii = RespMsgProcessor.ReadInt32( strm )


[<Property>]
let ``ReadInt64 basic`` (ii:int64) =
    use strm = new System.IO.MemoryStream()
    let bs = sprintf "%d\r\n" ii |> Utils.StrToBytes 
    do  strm.Write( bs, 0, bs.Length)
    let _ = strm.Seek(0L, System.IO.SeekOrigin.Begin)
    let actual = RespMsgProcessor.ReadInt64( strm )
    ii = actual


//[<Fact>]
//let ``ReadInt32 at end of stream returns zero`` () =
//    use strm = new System.IO.MemoryStream()
//    test <@ 0 = RespMsgProcessor.ReadInt32( strm ) @>


//[<Fact>]
//let ``ReadInt64 at end of stream returns zero`` () =
//    use strm = new System.IO.MemoryStream()
//    test <@ 0L = RespMsgProcessor.ReadInt64( strm ) @>



