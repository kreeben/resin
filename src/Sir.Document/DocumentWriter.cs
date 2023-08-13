using Sir.IO;
using Sir.KeyValue;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.Documents
{
    /// <summary>
    /// Writes documents to a database.
    /// </summary>
    public class DocumentWriter : KeyValueWriter, IDisposable
    {
        private readonly DocMapWriter _docs;
        private readonly DocIndexWriter _docIx;
        
        public DocumentWriter(ISessionFactory sessionFactory, string directory, ulong collectionId) 
            : base(directory, collectionId, sessionFactory)
        {
            var documentStream = sessionFactory.CreateAppendStream(directory, collectionId, "docs");
            var documentIndexStream = sessionFactory.CreateAppendStream(directory, collectionId, "dix");

            _docs = new DocMapWriter(documentStream);
            _docIx = new DocIndexWriter(documentIndexStream);
        }

        public DocumentWriter(ulong collectionId, Stream documentStream, Stream documentIndexStream, Stream valueStream, Stream keyStream, Stream valueIndexStream, Stream keyIndexStream, KeyRepository keyRepository)
            : base(collectionId, new ValueWriter(valueStream), new ValueWriter(keyStream), new ValueIndexWriter(valueIndexStream), new ValueIndexWriter(keyIndexStream), keyRepository)
        {
            _docs = new DocMapWriter(documentStream);
            _docIx = new DocIndexWriter(documentIndexStream);
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

        public override void Dispose()
        {
            base.Dispose();

            _docs.Dispose();
            _docIx.Dispose();
        }
    }
}
