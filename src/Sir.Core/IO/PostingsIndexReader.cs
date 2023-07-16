using System;
using System.Buffers;
using System.IO;

namespace Sir.IO
{
    public class PostingsIndexReader : IDisposable
    {
        private readonly Stream _postingsIndex;

        public PostingsIndexReader(Stream postingsIndex)
        {
            _postingsIndex = postingsIndex;
        }

        public (long address, long nextPageId) GetPageInfo(long pageId)
        {
            _postingsIndex.Seek(pageId * PostingsIndexAppender.BlockSize, SeekOrigin.Begin);
            var buf = ArrayPool<byte>.Shared.Rent(PostingsIndexAppender.BlockSize);
            var read = _postingsIndex.Read(buf, 0, PostingsIndexAppender.BlockSize);

            if (read != PostingsIndexAppender.BlockSize)
                throw new Exception($"Expected to read {PostingsIndexAppender.BlockSize} but instead read {read}.");

            var address = BitConverter.ToInt64(buf);
            var nextPageId = BitConverter.ToInt64(buf, sizeof(long));
            ArrayPool<byte>.Shared.Return(buf);

            return (address, nextPageId);
        }

        public void Dispose()
        {
            if (_postingsIndex != null)
                _postingsIndex.Dispose();
        }
    }
}
