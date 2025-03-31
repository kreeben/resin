namespace Resin.KeyValue
{
    public class Int64Reader : ByteArrayReader<long>
    {
        public Int64Reader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(long), pageSize)
        {
        }
    }

    public class Int32Reader : ByteArrayReader<int>
    {
        public Int32Reader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(int), pageSize)
        {
        }
    }

    public class Int128Reader : ByteArrayReader<Int128>
    {
        public Int128Reader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, 16, pageSize)
        {
        }
    }

    public class SingleReader : ByteArrayReader<float>
    {
        public SingleReader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(float), pageSize)
        {
        }
    }

    public class Int16Reader : ByteArrayReader<Int16>
    {
        public Int16Reader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(Int16), pageSize)
        {
        }
    }

    public class ByteReader : ByteArrayReader<byte>
    {
        public ByteReader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(byte), pageSize)
        {
        }
    }

    public class DoubleReader : ByteArrayReader<double>
    {
        public DoubleReader(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, sizeof(double), pageSize)
        {
        }
    }
}
