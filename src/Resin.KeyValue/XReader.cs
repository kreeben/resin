namespace Resin.KeyValue
{
    public class Int64Reader : PageReader<long>
    {
        public Int64Reader(ReadSession readSession, int pageSize)
            : base(readSession, sizeof(long), pageSize)
        {
        }
    }

    public class Int32Reader : PageReader<int>
    {
        public Int32Reader(ReadSession readSession, int pageSize)
            : base(readSession, sizeof(int), pageSize)
        {
        }
    }

    public class Int128Reader : PageReader<Int128>
    {
        public Int128Reader(ReadSession readSession, int pageSize)
            : base(readSession, 16, pageSize)
        {
        }
    }

    public class SingleReader : PageReader<float>
    {
        public SingleReader(ReadSession readSession, int pageSize)
            : base(readSession, sizeof(float), pageSize)
        {
        }
    }

    public class Int16Reader : PageReader<Int16>
    {
        public Int16Reader(ReadSession readSession, int pageSize)
            : base(readSession, sizeof(Int16), pageSize)
        {
        }
    }

    public class ByteReader : PageReader<byte>
    {
        public ByteReader(ReadSession readSession, int pageSize)
            : base(readSession, sizeof(byte), pageSize)
        {
        }
    }

    public class DoubleReader : PageReader<double>
    {
        public DoubleReader(ReadSession readSession, int pageSize)
            : base(readSession, sizeof(double), pageSize)
        {
        }
    }
}
