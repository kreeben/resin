using System;
using System.IO;

namespace Resin.IO.Write
{
    public abstract class BlockWriter<T> : IDisposable
    {
        protected abstract byte[] Serialize(T block);

        private long _position;
        private readonly Stream _stream;

        public BlockWriter(Stream stream)
        {
            _position = 0;
            _stream = stream;
        }

        public BlockInfo Write(T block)
        {
            var bytes = Serialize(block);

            if (bytes.Length == 0) throw new InvalidTimeZoneException();

            var info = new BlockInfo(_position, bytes.Length);

            _stream.Write(bytes, 0, bytes.Length);
            _position += bytes.Length;

            return info;
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}