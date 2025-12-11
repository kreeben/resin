namespace Resin.KeyValue
{
    public class Int64Reader : PageReader<long>
    {
        public Int64Reader(ReadSession readSession)
            : base(readSession, sizeof(long))
        {
        }
    }

    public class Int32Reader : PageReader<int>
    {
        public Int32Reader(ReadSession readSession)
            : base(readSession, sizeof(int))
        {
        }
    }

    public class Int128Reader : PageReader<Int128>
    {
        public Int128Reader(ReadSession readSession)
            : base(readSession, 16)
        {
        }
    }

    public class SingleReader : PageReader<float>
    {
        public SingleReader(ReadSession readSession)
            : base(readSession, sizeof(float))
        {
        }
    }

    public class Int16Reader : PageReader<Int16>
    {
        public Int16Reader(ReadSession readSession)
            : base(readSession, sizeof(Int16))
        {
        }
    }

    public class ByteReader : PageReader<byte>
    {
        public ByteReader(ReadSession readSession)
            : base(readSession, sizeof(byte))
        {
        }
    }

    public class DoubleReader : PageReader<double>
    {
        public DoubleReader(ReadSession readSession)
            : base(readSession, sizeof(double))
        {
        }
    }
}
