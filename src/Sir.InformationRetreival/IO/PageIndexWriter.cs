using System;
using System.IO;

namespace Sir.IO
{
    /// <summary>
    /// Index segment address writer.
    /// </summary>
    public class PageIndexWriter : IDisposable
    {
        private readonly Stream _stream;
        private readonly bool _keepStreamOpen;

        public PageIndexWriter(Stream stream, bool keepStreamOpen = false)
        {
            _stream = stream;
            _keepStreamOpen = keepStreamOpen;
        }

        public void Put(long offset, long length)
        {
            _stream.Write(BitConverter.GetBytes(offset));
            _stream.Write(BitConverter.GetBytes(length));
        }

        public void Dispose()
        {
            if(!_keepStreamOpen)
                _stream.Dispose();
        }
    }
}