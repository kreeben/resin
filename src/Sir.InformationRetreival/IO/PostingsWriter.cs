using System;
using System.IO;

namespace Sir.IO
{
    public class PostingsWriter : IDisposable
    {
        private readonly Stream _postingsStream;

        public PostingsWriter(Stream postingsStream)
        {
            _postingsStream = postingsStream;
        }

        public void SerializePostings(VectorNode node)
        {
            if (node.DocIds.Count == 0) throw new ArgumentException("can't be empty", nameof(node.DocIds));

            node.PostingsOffset = _postingsStream.Position;

            // serialize item count
            _postingsStream.Write(BitConverter.GetBytes((long)node.DocIds.Count));

            // serialize address of next page (unknown at this time)
            _postingsStream.Write(BitConverter.GetBytes((long)0));

            foreach (var docId in node.DocIds)
            {
                _postingsStream.Write(BitConverter.GetBytes(docId));
            }
        }

        public void Dispose()
        {
            if (_postingsStream != null )
            {
                _postingsStream.Dispose();
            }
        }
    }
}