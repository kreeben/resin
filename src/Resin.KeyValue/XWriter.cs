namespace Resin.KeyValue
{
    public class Int64Writer : PageWriter<long>
    {
        public Int64Writer(WriteSession writeTransaction)
            : base(writeTransaction)
        {
        }
    }

    public class Int32Writer : PageWriter<int>
    {
        public Int32Writer(WriteSession writeTransaction)
            : base(writeTransaction)
        {
        }
    }

    public class Int128Writer : PageWriter<Int128>
    {
        public Int128Writer(WriteSession writeTransaction)
            : base(writeTransaction)
        {
        }
    }

    public class SingleWriter : PageWriter<float>
    {
        public SingleWriter(WriteSession writeTransaction)
            : base(writeTransaction)
        {
        }
    }

    public class Int16Writer : PageWriter<Int16>
    {
        public Int16Writer(WriteSession writeTransaction)
            : base(writeTransaction)
        {
        }
    }

    public class ByteWriter : PageWriter<byte>
    {
        public ByteWriter(WriteSession writeTransaction)
            : base(writeTransaction)
        {
        }
    }

    public class DoubleWriter : PageWriter<double>
    {
        public DoubleWriter(WriteSession writeTransaction)
            : base(writeTransaction)
        {
        }
    }
}