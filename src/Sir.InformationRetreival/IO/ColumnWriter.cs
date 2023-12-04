using System;
using System.IO;

namespace Sir.IO
{
    public class ColumnWriter : IDisposable
    {
        private readonly Stream _ixStream;
        private readonly bool _keepIndexStreamOpen;

        public ColumnWriter(Stream indexStream, bool keepStreamOpen = false)
        {
            _ixStream = indexStream;
            _keepIndexStreamOpen = keepStreamOpen;
        }

        public (int depth, int width) CreatePage(VectorNode column, Stream vectorStream, Stream postingsStream, PageIndexWriter pageIndexWriter)
        {
            var page = column.SerializeTree(_ixStream, vectorStream, postingsStream);

            pageIndexWriter.Put(page.offset, page.length);

            return PathFinder.Size(column);
        }

        public (int depth, int width) CreatePage(VectorNode column, Stream vectorStream, PageIndexWriter pageIndexWriter)
        {
            var page = column.SerializeTree(_ixStream, vectorStream, null);

            pageIndexWriter.Put(page.offset, page.length);

            return PathFinder.Size(column);
        }

        public void Dispose()
        {
            if (!_keepIndexStreamOpen)
                _ixStream.Dispose();
        }
    }
}