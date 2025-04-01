namespace Resin.KeyValue
{
    public class Int64Reader : PageReader<long>
    {
        public Int64Reader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(long), pageSize)
        {
        }
    }

    public class Int32Reader : PageReader<int>
    {
        public Int32Reader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(int), pageSize)
        {
        }
    }

    public class Int128Reader : PageReader<Int128>
    {
        public Int128Reader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, 16, pageSize)
        {
        }
    }

    public class SingleReader : PageReader<float>
    {
        public SingleReader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(float), pageSize)
        {
        }
    }

    public class Int16Reader : PageReader<Int16>
    {
        public Int16Reader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(Int16), pageSize)
        {
        }
    }

    public class ByteReader : PageReader<byte>
    {
        public ByteReader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(byte), pageSize)
        {
        }
    }

    public class DoubleReader : PageReader<double>
    {
        public DoubleReader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(double), pageSize)
        {
        }
    }
}
