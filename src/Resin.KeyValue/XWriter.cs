namespace Resin.KeyValue
{
    public class Int64Writer : ByteArrayWriter<long>
    {
        public Int64Writer(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, long.MaxValue, (x) => BitConverter.GetBytes(x), sizeof(long), pageSize)
        {
        }
    }

    public class Int32Writer : ByteArrayWriter<int>
    {
        public Int32Writer(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, int.MaxValue, (x) => BitConverter.GetBytes(x), sizeof(int), pageSize)
        {
        }
    }

    public class Int128Writer : ByteArrayWriter<Int128>
    {
        public Int128Writer(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, Int128.MaxValue, (x) => BitConverter.GetBytes(x), 16, pageSize)
        {
        }
    }

    public class SingleWriter : ByteArrayWriter<float>
    {
        public SingleWriter(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, float.MaxValue, (x) => BitConverter.GetBytes(x), sizeof(float), pageSize)
        {
        }
    }

    public class Int16Writer : ByteArrayWriter<Int16>
    {
        public Int16Writer(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, Int16.MaxValue, (x) => BitConverter.GetBytes(x), sizeof(Int16), pageSize)
        {
        }
    }

    public class ByteWriter : ByteArrayWriter<byte>
    {
        public ByteWriter(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, byte.MaxValue, (x) => [x], sizeof(byte), pageSize)
        {
        }
    }

    public class DoubleWriter : ByteArrayWriter<double>
    {
        public DoubleWriter(Stream keyStream, Stream valueStream, Stream addressStream, int pageSize)
            : base(keyStream, valueStream, addressStream, double.MaxValue, (x) => BitConverter.GetBytes(x), sizeof(double), pageSize)
        {
        }
    }
}