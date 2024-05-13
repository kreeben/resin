using System;
using System.IO;

namespace Sir.IO
{
    public class PostingsWriter : IDisposable
    {
        private readonly Stream _stream;
        private readonly IndexCache _indexCache;

        public PostingsWriter(Stream postingsStream, IndexCache indexCache = null)
        {
            _stream = postingsStream;
            _indexCache = indexCache;

            _stream.Seek(0, SeekOrigin.End);
        }

        public long SerializePostings(VectorNode node)
        {
            if (node.DocIds.Count == 0) throw new ArgumentException("can't be empty", nameof(node.DocIds));

            /* --------------- */
            /* write new page */
            /* ------------- */

            // store stream position
            var postingsOffset = _stream.Position;

            // serialize postings count
            _stream.Write(BitConverter.GetBytes((long)node.DocIds.Count));

            // serialize address of next page (unknown at this time)
            _stream.Write(BitConverter.GetBytes((long)0));

            // serialize document IDs
            foreach (var docId in node.DocIds)
            {
                _stream.Write(BitConverter.GetBytes(docId));
            }

            long? existingPostingsOffset = null;

            if (_indexCache != null)
            {
                existingPostingsOffset = _indexCache.GetPostingsOffset(node.KeyId.Value, node.Vector);
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
                _indexCache.UpdatePostingsOffset(node.KeyId.Value, node.Vector, postingsOffset);
            }
            else if (_indexCache != null)
            {
                _indexCache.Put(new VectorNode(vector: node.Vector, postingsOffset: postingsOffset, keyId: node.KeyId));
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