using Sir.Documents;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir.IO
{
    public class PostingsWriter : IDisposable
    {
        private readonly Stream _stream;
        private readonly WriteSession _writeSession;
        private readonly IndexCache _indexCache;
        private readonly Stopwatch _postingsTimer;
        private readonly Stopwatch _headerTimer;
        private readonly Stopwatch _cacheTimer;

        public PostingsWriter(Stream stream, WriteSession writeSession, IndexCache indexCache = null)
        {
            if (!stream.CanSeek) throw new ArgumentException(nameof(stream));
            if (!stream.CanWrite) throw new ArgumentException(nameof(stream));

            _stream = stream;
            _writeSession = writeSession;
            _indexCache = indexCache;
            _stream.Seek(0, SeekOrigin.End);
            _postingsTimer = new Stopwatch();
            _headerTimer = new Stopwatch();
            _cacheTimer = new Stopwatch();
        }

        public (TimeSpan postings, TimeSpan headers, TimeSpan cache) GetTimings() 
        { 
            return (_postingsTimer.Elapsed, _headerTimer.Elapsed, _cacheTimer.Elapsed); 
        }

        public long SerializePostings(IList<long> documents, long keyId, ISerializableVector term)
        {
            if (documents is null) throw new ArgumentNullException(nameof(documents));
            if (documents.Count == 0) throw new ArgumentException("can't be empty", nameof(documents));

            /* --------------- */
            /* write new page */
            /* ------------- */
            _postingsTimer.Start();
            var listId = _writeSession.Put(new Document(new List<Field> { new Field("postings", documents) }));
            _postingsTimer.Stop();

            /* ----------------- */
            /* write new header */
            /* --------------- */
            // go to end of stream
            _headerTimer.Start();
            _stream.Seek(0, SeekOrigin.End);
            var postingsHeaderOffset = _stream.Position;

            // serialize postings list ID
            _stream.Write(BitConverter.GetBytes(listId));

            // serialize address of next page (unknown at this time)
            _stream.Write(BitConverter.GetBytes((long)0));
            _headerTimer.Stop();

            // is there an existing postings list for this term?
            long? existingPostingsOffset = null;

            if (_indexCache != null)
            {
                existingPostingsOffset = _indexCache.GetPostingsOffset(keyId, term);
            }

            if (existingPostingsOffset.HasValue && existingPostingsOffset.Value > 0)
            {
                /* ------------------------------------ */
                /* reference new page in existing page */
                /* ---------------------------------- */
                _headerTimer.Start();
                // seek to header
                _stream.Seek(existingPostingsOffset.Value+sizeof(long), SeekOrigin.Begin);

                // set new page as next page
                _stream.Write(BitConverter.GetBytes(postingsHeaderOffset));
                _headerTimer.Stop();

                // set this page as cached offset
                _cacheTimer.Start();
                _indexCache.UpdatePostingsOffset(keyId, term, postingsHeaderOffset);
                _cacheTimer.Stop();
            }
            else if (_indexCache != null)
            {
                _cacheTimer.Start();
                _indexCache.Put(new VectorNode(vector: term, postingsOffset: postingsHeaderOffset, keyId: keyId));
                _cacheTimer.Stop();
            }

            return postingsHeaderOffset;
        }

        public void Dispose()
        {
            if (_stream != null )
            {
                _stream.Dispose();
            }
        }
    }
}