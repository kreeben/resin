using Sir.Documents;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.IO
{
    public class PostingsWriter : IDisposable
    {
        private readonly Stream _stream;
        private readonly WriteSession _writeSession;
        private readonly IndexCache _indexCache;

        public PostingsWriter(Stream stream, WriteSession writeSession, IndexCache indexCache = null)
        {
            if (!stream.CanSeek) throw new ArgumentException(nameof(stream));
            if (!stream.CanWrite) throw new ArgumentException(nameof(stream));

            _stream = stream;
            _writeSession = writeSession;
            _indexCache = indexCache;
            _stream.Seek(0, SeekOrigin.End);
        }

        public long SerializePostings(IList<long> documents, long keyId, ISerializableVector term)
        {
            if (documents is null) throw new ArgumentNullException(nameof(documents));
            if (documents.Count == 0) throw new ArgumentException("can't be empty", nameof(documents));

            // go to end of stream
            _stream.Seek(0, SeekOrigin.End);

            /* --------------- */
            /* write new page */
            /* ------------- */
            var listId = _writeSession.Put(new Document(new List<Field> { new Field("postings", documents) }));

            /* ----------------- */
            /* write new header */
            /* --------------- */

            var postingsHeaderOffset = _stream.Position;

            // serialize postings list ID
            _stream.Write(BitConverter.GetBytes(listId));

            // serialize address of next page (unknown at this time)
            _stream.Write(BitConverter.GetBytes((long)0));

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

                // seek to header
                _stream.Seek(existingPostingsOffset.Value+sizeof(long), SeekOrigin.Begin);

                // set new page as next page
                _stream.Write(BitConverter.GetBytes(postingsHeaderOffset));

                // set this page as cached offset
                _indexCache.UpdatePostingsOffset(keyId, term, postingsHeaderOffset);
            }
            else if (_indexCache != null)
            {
                _indexCache.Put(new VectorNode(vector: term, postingsOffset: postingsHeaderOffset, keyId: keyId));
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