using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.IO
{
    public class PostingsWriter : IDisposable
    {
        private readonly Stream _stream;
        private readonly IndexCache _indexCache;

        public PostingsWriter(Stream stream, IndexCache indexCache = null)
        {
            if (!stream.CanSeek) throw new ArgumentException(nameof(stream));
            if (!stream.CanWrite) throw new ArgumentException(nameof(stream));

            _stream = stream;
            _indexCache = indexCache;
            _stream.Seek(0, SeekOrigin.End);
        }

        public long SerializePostings(IList<long> documents, long keyId, ISerializableVector vector)
        {
            if (documents is null) throw new ArgumentNullException(nameof(documents));
            if (documents.Count == 0) throw new ArgumentException("can't be empty", nameof(documents));

            /* --------------- */
            /* write new page */
            /* ------------- */

            // store stream position
            var postingsOffset = _stream.Position;

            // serialize postings count
            _stream.Write(BitConverter.GetBytes((long)documents.Count));

            // serialize address of next page (unknown at this time)
            _stream.Write(BitConverter.GetBytes((long)0));

            // serialize document IDs
            foreach (var docId in documents)
            {
                _stream.Write(BitConverter.GetBytes(docId));
            }

            long? existingPostingsOffset = null;

            if (_indexCache != null)
            {
                existingPostingsOffset = _indexCache.GetPostingsOffset(keyId, vector);
            }

            if (existingPostingsOffset.HasValue && existingPostingsOffset.Value > 0)
            {
                /* ------------------------------------ */
                /* reference new page in existing page */
                /* ---------------------------------- */

                // rewind stream to existing postings page header
                _stream.Seek(existingPostingsOffset.Value+sizeof(long), SeekOrigin.Begin);

                // set this as next page of existing postings page
                _stream.Write(BitConverter.GetBytes(postingsOffset));

                // go back to end of stream
                _stream.Seek(0, SeekOrigin.End);

                // set this as offset of existing postings page
                _indexCache.UpdatePostingsOffset(keyId, vector, postingsOffset);
            }
            else if (_indexCache != null)
            {
                _indexCache.Put(new VectorNode(vector: vector, postingsOffset: postingsOffset, keyId: keyId));
            }
            return postingsOffset;
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