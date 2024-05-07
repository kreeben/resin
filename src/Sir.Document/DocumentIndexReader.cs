using System;
using System.Buffers;
using System.IO;

namespace Sir.Documents
{
    /// <summary>
    /// Lookup the offset and length of a document map consisting of key IDs and value IDs.
    /// </summary>
    public class DocumentIndexReader : IDisposable
    {
        private readonly Stream _stream;

        public int Count
        {
            get
            {
                return ((int)_stream.Length / DocumentIndexWriter.BlockSize);
            }
        }

        public DocumentIndexReader(Stream stream)
        {
            _stream = stream;
        }

        /// <summary>
        /// Get the offset and length of a document's key_id/value_id map.
        /// </summary>
        /// <param name="docId">Document ID</param>
        /// <returns>The offset and length of a document's key_id/value_id map</returns>
        public (long offset, int length) Get(long docId)
        {
            var offs = docId * DocumentIndexWriter.BlockSize;

            _stream.Seek(offs, SeekOrigin.Begin);

            var buf = ArrayPool<byte>.Shared.Rent(DocumentIndexWriter.BlockSize);

            var read = _stream.Read(buf);

            if (read == 0)
            {
                throw new ArgumentException(nameof(docId));
            }

            var address = (BitConverter.ToInt64(buf), BitConverter.ToInt32(buf, sizeof(long)));

            ArrayPool<byte>.Shared.Return(buf);

            return address;
        }

        public void Dispose()
        {
            if (_stream != null)
                _stream.Dispose();
        }
    }
}