namespace Resin.KeyValue
{
    public class Int64Writer : PageWriter<long>
    {
        public Int64Writer(WriteTransaction writeTransaction, int pageSize, bool batchMode = false)
            : base(writeTransaction, long.MaxValue, (x) => BitConverter.GetBytes(x), sizeof(long), pageSize, batchMode)
        {
        }
    }

    public class Int32Writer : PageWriter<int>
    {
        public Int32Writer(WriteTransaction writeTransaction, int pageSize, bool batchMode = false)
            : base(writeTransaction, int.MaxValue, (x) => BitConverter.GetBytes(x), sizeof(int), pageSize, batchMode)
        {
        }
    }

    public class Int128Writer : PageWriter<Int128>
    {
        public Int128Writer(WriteTransaction writeTransaction, int pageSize, bool batchMode = false)
            : base(writeTransaction, Int128.MaxValue, (x) => BitConverter.GetBytes(x), 16, pageSize, batchMode)
        {
        }
    }

    public class SingleWriter : PageWriter<float>
    {
        public SingleWriter(WriteTransaction writeTransaction, int pageSize, bool batchMode = false)
            : base(writeTransaction, float.MaxValue, (x) => BitConverter.GetBytes(x), sizeof(float), pageSize, batchMode)
        {
        }
    }

    public class Int16Writer : PageWriter<Int16>
    {
        public Int16Writer(WriteTransaction writeTransaction, int pageSize, bool batchMode = false)
            : base(writeTransaction, Int16.MaxValue, (x) => BitConverter.GetBytes(x), sizeof(Int16), pageSize, batchMode)
        {
        }
    }

    public class ByteWriter : PageWriter<byte>
    {
        public ByteWriter(WriteTransaction writeTransaction, int pageSize, bool batchMode = false)
            : base(writeTransaction, byte.MaxValue, (x) => [x], sizeof(byte), pageSize, batchMode)
        {
        }
    }

    public class DoubleWriter : PageWriter<double>
    {
        public DoubleWriter(WriteTransaction writeTransaction, int pageSize, bool batchMode = false)
            : base(writeTransaction, double.MaxValue, (x) => BitConverter.GetBytes(x), sizeof(double), pageSize, batchMode)
        {
        }
    }
}