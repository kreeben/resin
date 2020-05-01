using System;
using System.IO;

namespace Sir.VectorSpace
{
    public class ColumnStreamWriter : IDisposable
    {
        private readonly long _keyId;
        private readonly ulong _collectionId;
        private static readonly object _indexFileSync = new object();
        private readonly Stream _ixStream;

        public ColumnStreamWriter(
            ulong collectionId, 
            long keyId,
            Stream indexStream)
        {
            _keyId = keyId;
            _collectionId = collectionId;
            _ixStream = indexStream;
        }

        public (int depth, int width) CreatePage(VectorNode column, Stream vectorStream, Stream postingsStream, PageIndexWriter pageIndexWriter)
        {
            var page = GraphBuilder.SerializeTree(column, _ixStream, vectorStream, postingsStream);

            pageIndexWriter.Put(page.offset, page.length);

            return PathFinder.Size(column);
        }

        public void Dispose()
        {
            _ixStream.Dispose();
        }
    }
}