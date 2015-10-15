using System;
using System.Runtime.InteropServices;

public static class Padded
{
    private const int CacheLineSize = 64;


    [StructLayout(LayoutKind.Explicit, Size = CacheLineSize * 2)]
    public class Sequence
    {
        [FieldOffset(0)]
        public long _value;

        public Sequence(long value)
        {
            _value = value;
        }

        public override string ToString()
        {
            return String.Format("{0}", _value);
        }
    }



    // Additional information: Could not load type 'DistEvent`1' from assembly 'Sequence, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null' 
    // because generic types cannot have explicit layout


    [StructLayout(LayoutKind.Explicit, Size = CacheLineSize * 2)]
    public struct DistEvent
    {
        [FieldOffset(0)]
        public object _value;

        public override string ToString()
        {
            return String.Format("{0}", _value);
        }
    }

}

