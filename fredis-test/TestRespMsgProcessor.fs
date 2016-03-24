module TestRespMsgParser



open Xunit
open FsCheck
open FsCheck.Xunit
open Swensen.Unquote


// https://code.google.com/p/fsunit/


[<Fact>]
let ``ReadUntilCRLF basic`` () =
    use strm = new System.IO.MemoryStream()
    let bs = "678\r\n"B
    strm.Write( bs, 0, bs.Length)
    strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
    test <@ [54;55;56] = RespMsgParser.ReadUntilCRLF( strm ) @>



[<Fact>]
let ``ReadUntilCRLF not empty after CRLF`` () =
    use strm = new System.IO.MemoryStream()
    let bs = "678\r\n123"B
    strm.Write( bs, 0, bs.Length)
    strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
    test <@ [54;55;56] = RespMsgParser.ReadUntilCRLF( strm ) @>




[<Property>]
let ``ReadInt32 basic`` (ii:int32) =
    use strm = new System.IO.MemoryStream()
    let bs = sprintf "%d\r\n" ii |> Utils.StrToBytes 
    do  strm.Write( bs, 0, bs.Length)
    strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
    ii = RespMsgParser.ReadInt32( strm )


[<Property>]
let ``ReadInt64 basic`` (ii:int64) =
    use strm = new System.IO.MemoryStream()
    let bs = sprintf "%d\r\n" ii |> Utils.StrToBytes 
    do  strm.Write( bs, 0, bs.Length)
    strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
    let actual = RespMsgParser.ReadInt64( strm )
    ii = actual







