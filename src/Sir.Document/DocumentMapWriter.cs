using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.Documents
{
    /// <summary>
    /// Writes document maps (key_id/val_id) to a bitmap.
    /// </summary>
    public class DocumentMapWriter : IDisposable
    {
        private readonly Stream _stream;

        public DocumentMapWriter(Stream stream)
        {
            _stream = stream;
        }

        public void Flush()
        {
            _stream.Flush();
        }

        public (long offset, int length) Put(IList<(long keyId, long valId)> doc)
        {
            if (doc.Count == 0)
                throw new ArgumentException(nameof(doc));

            var off = _stream.Position;

            foreach (var kv in doc)
            {
                _stream.Write(BitConverter.GetBytes(kv.keyId));
                _stream.Write(BitConverter.GetBytes(kv.valId));
            }

            return (off, sizeof(long) * 2 * doc.Count);
        }

        public void Overwrite(long offsetOfMap, int indexInMap, long keyId, long valId)
        {
            // seek to the value part of the key/value block that is referenced by indexInMap
            _stream.Seek(offsetOfMap + (indexInMap*(sizeof(long)*2)) + sizeof(long), SeekOrigin.Begin);

            // overwrite
            _stream.Write(BitConverter.GetBytes(valId));
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
