using System;
using System.IO;

namespace Sir.Store
{
    /// <summary>
    /// Store the location of a value.
    /// </summary>
    public class ValueIndexWriter : IDisposable
    {
        private readonly Stream _stream;
        private static int _blockSize = sizeof(long) + sizeof(int) + sizeof(byte);

        public ValueIndexWriter(Stream stream)
        {
            _stream = stream;
        }

        public long Append(long offset, int len, byte dataType)
        {
            var position = _stream.Position;

            _stream.Write(BitConverter.GetBytes(offset));
            _stream.Write(BitConverter.GetBytes(len));
            _stream.WriteByte(dataType);
            _stream.Flush();

            return position == 0 ? 0 : position / _blockSize;
        }

        public void Dispose()
        {
        }
    }
}
