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
        private DocumentMapWriter _docs;
        private DocumentIndexWriter _docIx;
        private KeyValueWriter _kvWriter;
        private readonly string _directory;
        private readonly ulong _collectionId;

        public KeyValueWriter KeyValueWriter { get { return _kvWriter; } }

        public DocumentRegistryWriter(string directory, ulong collectionId)
        {
            _docs = new DocumentMapWriter(KeyValueWriter.CreateAppendStream(directory, collectionId, "docs"));
            _docIx = new DocumentIndexWriter(KeyValueWriter.CreateAppendStream(directory, collectionId, "dix"));
            _kvWriter = new KeyValueWriter(directory, collectionId);
            _directory = directory;
            _collectionId = collectionId;
        }

        public long IncrementDocId()
        {
            return _docIx.IncrementDocId();
        }

        public (long offset, int length) PutDocumentMap(IList<(long keyId, long valId)> doc)
        {
            return _docs.Put(doc);
        }

        public void UpdateDocumentMap(long offsetOfMap, int indexInMap, long keyId, long valId)
        {
            _docs.Overwrite(offsetOfMap, indexInMap, keyId, valId);
        }

        public void PutDocumentAddress(long docId, long offset, int len)
        {
            _docIx.Put(docId, offset, len);
        }

        public void Commit()
        {
            _docs.Dispose();
            _docIx.Dispose();
            _kvWriter.Dispose();

            _docs = new DocumentMapWriter(KeyValueWriter.CreateAppendStream(_directory, _collectionId, "docs"));
            _docIx = new DocumentIndexWriter(KeyValueWriter.CreateAppendStream(_directory, _collectionId, "dix"));
            _kvWriter = new KeyValueWriter(_directory, _collectionId);
        }

        public void Dispose()
        {
            _docs.Dispose();
            _docIx.Dispose();
            _kvWriter.Dispose();
        }
    }
}
