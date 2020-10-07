﻿using System;
using System.IO;

namespace Sir.KeyValue
{
    /// <summary>
    /// Find the address of a value by supplying a value ID.
    /// </summary>
    public class ValueIndexReader : IDisposable
    {
        private readonly Stream _stream;
        private static int _blockSize = sizeof(long) + sizeof(int) + sizeof(byte);

        public ValueIndexReader(Stream stream)
        {
            _stream = stream;
        }

        public void Dispose()
        {
            if (_stream != null)
                _stream.Dispose();
        }

        public (long offset, int len, byte dataType) Get(long id)
        {
            var offset = id * _blockSize;

            _stream.Seek(offset, SeekOrigin.Begin);

            Span<byte> buf = stackalloc byte[_blockSize];
            var read =  _stream.Read(buf);

            if (read != _blockSize)
            {
                throw new InvalidDataException();
            }

            return (BitConverter.ToInt64(buf.Slice(0)), BitConverter.ToInt32(buf.Slice(sizeof(long))), buf[_blockSize - 1]);
        }
    }
}
