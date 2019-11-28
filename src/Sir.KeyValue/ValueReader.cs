using System;
using System.IO;

namespace Sir.KeyValue
{
    /// <summary>
    /// Read a value by supplying an offset, length and data type.
    /// </summary>
    public class ValueReader : IDisposable
    {
        private readonly Stream _stream;

        public ValueReader(Stream stream)
        {
            _stream = stream;
        }

        public void Dispose()
        {
            if (_stream != null)
                _stream.Dispose();
        }

        public object Get(long offset, int len, byte dataType)
        {
            int read;
            Span<byte> buf = stackalloc byte[len];

            _stream.Seek(offset, SeekOrigin.Begin);

            read = _stream.Read(buf);

            if (read != len)
            {
                throw new InvalidDataException();
            }

            var typeId = Convert.ToInt32(dataType);

            if (DataType.BOOL == typeId)
            {
                return Convert.ToBoolean(buf[0]);
            }
            else if (DataType.CHAR == typeId)
            {
                return BitConverter.ToChar(buf);
            }
            else if (DataType.FLOAT == typeId)
            {
                return BitConverter.ToSingle(buf);
            }
            else if (DataType.INT == typeId)
            {
                return BitConverter.ToInt32(buf);
            }
            else if (DataType.DOUBLE == typeId)
            {
                return BitConverter.ToDouble(buf);
            }
            else if (DataType.LONG == typeId)
            {
                return BitConverter.ToInt64(buf);
            }
            else if (DataType.DATETIME == typeId)
            {
                return DateTime.FromBinary(BitConverter.ToInt64(buf));
            }
            else if (DataType.STRING == typeId)
            {
                return new string(System.Text.Encoding.Unicode.GetChars(buf.ToArray()));
            }
            else
            {
                return buf.ToArray();
            }
        }
    }
}