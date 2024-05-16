using Sir.KeyValue;
using System;
using System.Collections.Generic;

namespace Sir.Documents
{
    /// <summary>
    /// Writes documents to a database.
    /// </summary>
    public class DocumentRegistryWriter : IDisposable
    {
        private DocumentMapWriter _documentMapWriter;
        private DocumentIndexWriter _documentIndexWriter;
        private KeyValueWriter _kvWriter;
        private readonly string _directory;
        private readonly ulong _collectionId;

        public KeyValueWriter KeyValueWriter { get { return _kvWriter; } }

        public DocumentRegistryWriter(string directory, ulong collectionId)
        {
            _documentMapWriter = new DocumentMapWriter(StreamFactory.CreateAppendStream(directory, collectionId, "docs"));
            _documentIndexWriter = new DocumentIndexWriter(StreamFactory.CreateAppendStream(directory, collectionId, "dix"));
            _kvWriter = new KeyValueWriter(directory, collectionId);
            _directory = directory;
            _collectionId = collectionId;
        }

        public long IncrementDocId()
        {
            return _documentIndexWriter.IncrementDocId();
        }

        public (long offset, int length) PutDocumentMap(IList<(long keyId, long valId)> doc)
        {
            return _documentMapWriter.Put(doc);
        }

        public void PutDocumentAddress(long docId, long offset, int len)
        {
            _documentIndexWriter.Put(docId, offset, len);
        }

        public void Commit()
        {
            _documentMapWriter.Dispose();
            _documentIndexWriter.Dispose();
            _kvWriter.Dispose();

            _documentMapWriter = new DocumentMapWriter(StreamFactory.CreateAppendStream(_directory, _collectionId, "docs"));
            _documentIndexWriter = new DocumentIndexWriter(StreamFactory.CreateAppendStream(_directory, _collectionId, "dix"));
            _kvWriter = new KeyValueWriter(_directory, _collectionId);
        }

        public void Dispose()
        {
            _documentMapWriter.Dispose();
            _documentIndexWriter.Dispose();
            _kvWriter.Dispose();
        }
    }
}
