using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.KeyValue
{
    /// <summary>
    /// Read a value in a stream by supplying an offset, length and data type.
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

        public IEnumerable<ISerializableVector> GetValueConvertedToVectors<T>(long offset, int len, byte dataType, Func<T, IEnumerable<ISerializableVector>> tokenizer)
        {
            int read;
            Span<byte> buf = new byte[len];

            _stream.Seek(offset, SeekOrigin.Begin);

            read = _stream.Read(buf);

            if (read != len)
            {
                throw new InvalidDataException();
            }

            var typeId = Convert.ToInt32(dataType);

            if (DataType.BOOL == typeId)
            {
                throw new NotImplementedException();
            }
            else if (DataType.CHAR == typeId)
            {
                throw new NotImplementedException();
            }
            else if (DataType.FLOAT == typeId)
            {
                throw new NotImplementedException();
            }
            else if (DataType.INT == typeId)
            {
                throw new NotImplementedException();
            }
            else if (DataType.DOUBLE == typeId)
            {
                throw new NotImplementedException();
            }
            else if (DataType.LONG == typeId)
            {
                throw new NotImplementedException();
            }
            else if (DataType.ULONG == typeId)
            {
                throw new NotImplementedException();
            }
            else if (DataType.DATETIME == typeId)
            {
                throw new NotImplementedException();
            }
            else if (DataType.STRING == typeId)
            {
                var charsLen = buf.Length / sizeof(char);
                Span<char> charBuf = new char[charsLen];
                
                System.Text.Encoding.Unicode.GetChars(buf, charBuf);

                var result = new string(charBuf);

                return tokenizer((T)(object)result);
            }
            else if (DataType.BYTE == typeId)
            {
                throw new NotImplementedException();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public object Get(long offset, int len, byte dataType)
        {
            var typeId = Convert.ToInt32(dataType);
            int read;
            var buf = new byte[len];

            _stream.Seek(offset, SeekOrigin.Begin);

            read = _stream.Read(buf);

            if (read != len)
            {
                throw new Exception($"offset: {offset} read: {read} should have read: {len} type: {typeId}");
            }

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
            else if (DataType.ULONG == typeId)
            {
                return BitConverter.ToUInt64(buf);
            }
            else if (DataType.DATETIME == typeId)
            {
                return DateTime.FromBinary(BitConverter.ToInt64(buf));
            }
            else if (DataType.STRING == typeId)
            {
                return new string(System.Text.Encoding.Unicode.GetChars(buf));
            }
            else if (DataType.BYTE == typeId)
            {
                return buf[0];
            }
            else if (DataType.LISTOFLONG == typeId)
            {
                return MemoryMarshal.Cast<byte, long>(buf).ToArray();
            }
            else // catches both STREAM and STREAMABLE
            {
                return buf;
            }
        }
    }
}