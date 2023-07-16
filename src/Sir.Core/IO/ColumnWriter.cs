using System;
using System.Collections.Generic;
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

        public (int depth, int width) CreatePage(
            VectorNode column,
            Stream vectorStream,
            PostingsWriter postingsWriter,
            PageIndexWriter pageIndexWriter, 
            Dictionary<(long keyId, long pageId), HashSet<long>> postingsToAppend)
        {
            if (postingsToAppend != null)
            {
                foreach (var posting in postingsToAppend)
                {
                    postingsWriter.AppendAndUpdatePageRef(posting.Key.pageId, posting.Value);
                }
            }

            var page = column.SerializeTree(_ixStream, vectorStream, postingsWriter);

            pageIndexWriter.Put(page.offset, page.length);

            return PathFinder.Size(column);
        }

        public (int depth, int width) CreatePage(
            VectorNode column, 
            Stream vectorStream, 
            PageIndexWriter pageIndexWriter)
        {
            var page = column.SerializeTree(_ixStream, vectorStream);

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