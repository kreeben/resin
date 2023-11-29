using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.IO
{
    public class PostingsWriter : IDisposable
    {
        private readonly Stream _postingsStream;
        private readonly PostingsIndexAppender _postingsIndexAppender;
        private readonly PostingsIndexUpdater _postingsIndexUpdater;
        private readonly PostingsIndexReader _postingsIndexReader;
        private readonly bool _keepOpen;

        public PostingsWriter(Stream postingsStream, PostingsIndexAppender postingsIndexAppender, PostingsIndexUpdater postingsIndexUpdater, PostingsIndexReader postingsIndexReader, bool keepOpen = false)
        {
            _postingsStream = postingsStream;
            _postingsIndexAppender = postingsIndexAppender;
            _postingsIndexReader = postingsIndexReader;
            _postingsIndexUpdater = postingsIndexUpdater;

            if (_postingsStream.Position != _postingsStream.Length)
            {
                _postingsStream.Position = _postingsStream.Length;
            }
            _keepOpen = keepOpen;
        }

        public long Append(HashSet<long> docIds)
        {
            if (docIds.Count == 0) throw new ArgumentException("can't be empty", nameof(docIds));

            // index page id
            var postingsOffset = _postingsStream.Position;
            var pageId = _postingsIndexAppender.Append(postingsOffset, -1);

            // serialize list size
            _postingsStream.Write(BitConverter.GetBytes((long)docIds.Count));

            // serialize list
            foreach (var docId in docIds)
            {
                _postingsStream.Write(BitConverter.GetBytes(docId));
            }

            return pageId;
        }

        public void AppendAndUpdatePageRef(long pageId, HashSet<long> docIds)
        {
            if (docIds.Count == 0) throw new ArgumentException("can't be empty", nameof(docIds));

            // write new page
            var newPageId = Append(docIds);

            // update parent page info
            _postingsIndexUpdater.Update(pageId, newPageId);
        }

        public void Dispose()
        {
            if (_keepOpen)
                return;

            if (_postingsStream != null)
                _postingsStream.Dispose();

            if (_postingsIndexAppender != null)
                _postingsIndexAppender.Dispose();

            if (_postingsIndexUpdater != null)
                _postingsIndexUpdater.Dispose();

            if (_postingsIndexReader != null)
                _postingsIndexReader.Dispose();
        }
    }
}
