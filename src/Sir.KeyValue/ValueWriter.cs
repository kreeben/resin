using System;
using System.IO;

namespace Sir.KeyValue
{
    /// <summary>
    /// Store a value on the file system.
    /// </summary>
    public class ValueWriter : IDisposable
    {
        private readonly Stream _stream;

        public ValueWriter(Stream stream)
        {
            _stream = stream;
        }

        public void Flush()
        {
            _stream.Flush();
        }

        public (long offset, int len, byte dataType) Put(object value)
        {
            Span<byte> buffer;
            byte dataType;

            if (value is bool)
            {
                buffer = BitConverter.GetBytes((bool)value);
                dataType = DataType.BOOL;
            }
            else if (value is char)
            {
                buffer = BitConverter.GetBytes((char)value);
                dataType = DataType.CHAR;
            }
            else if (value is float)
            {
                buffer = BitConverter.GetBytes((float)value);
                dataType = DataType.FLOAT;
            }
            else if (value is int)
            {
                buffer = BitConverter.GetBytes((int)value);
                dataType = DataType.INT;
            }
            else if (value is double)
            {
                buffer = BitConverter.GetBytes((double)value);
                dataType = DataType.DOUBLE;
            }
            else if (value is long)
            {
                buffer = BitConverter.GetBytes((long)value);
                dataType = DataType.LONG;
            }
            else if (value is DateTime)
            {
                buffer = BitConverter.GetBytes(((DateTime)value).ToBinary());
                dataType = DataType.DATETIME;
            }
            else if (value is string)
            {
                buffer = System.Text.Encoding.Unicode.GetBytes((string)value);
                dataType = DataType.STRING;
            }
            else
            {
                buffer = (byte[])value;
                dataType = DataType.STREAM;
            }

            var offset = _stream.Position;

            _stream.Write(buffer);

            return (offset, buffer.Length, dataType);
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
