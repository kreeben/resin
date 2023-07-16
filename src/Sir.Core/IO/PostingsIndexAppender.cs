using System;
using System.IO;

namespace Sir.IO
{
    public class PostingsIndexAppender : IDisposable
    {
        public static int BlockSize = sizeof(long)*2;
        private readonly Stream _appendableIndexStream;

        public PostingsIndexAppender(Stream appendableIndexStream)
        {
            _appendableIndexStream = appendableIndexStream;
        }

        public long Append(long address, long nextPageId)
        {
            long pageId = _appendableIndexStream.Length / BlockSize;
            _appendableIndexStream.Write(BitConverter.GetBytes(address));
            _appendableIndexStream.Write(BitConverter.GetBytes(nextPageId));
            return pageId;
        }

        public void Dispose()
        {
            if (_appendableIndexStream != null)
                _appendableIndexStream.Dispose();
        }
    }

    public class PostingsIndexUpdater : IDisposable
    {
        private readonly Stream _seekableIndexStream;

        public PostingsIndexUpdater(Stream seekableIndexStream)
        {
            _seekableIndexStream = seekableIndexStream;
        }

        public void Update(long pageId, long nextPageId)
        {
            _seekableIndexStream.Seek((pageId * PostingsIndexAppender.BlockSize) + sizeof(long), SeekOrigin.Begin);
            _seekableIndexStream.Write(BitConverter.GetBytes(nextPageId));
        }

        public void Dispose()
        {
            if (_seekableIndexStream != null)
                _seekableIndexStream.Dispose();
        }
    }
}
