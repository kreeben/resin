using Sir.Document;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Sir.Search
{
    public class DocumentStreamSession : IDisposable
    {
        protected readonly SessionFactory SessionFactory;
        private readonly ConcurrentDictionary<ulong, DocumentReader> _streamReaders;

        public DocumentStreamSession(SessionFactory sessionFactory) 
        {
            SessionFactory = sessionFactory;
            _streamReaders = new ConcurrentDictionary<ulong, DocumentReader>();
        }

        public virtual void Dispose()
        {
            foreach (var reader in _streamReaders.Values)
            {
                reader.Dispose();
            }
        }

        public IEnumerable<IDictionary<string, object>> ReadDocs(ulong collectionId, HashSet<string> select, int skip = 0, int take = int.MaxValue)
        {
            var documentReader = GetOrCreateDocumentReader(collectionId);
            var docCount = documentReader.DocumentCount();
            var took = 0;
            long docId = 1 + skip;

            while (docId <= docCount && took < take++)
            {
                yield return ReadDoc((collectionId, docId++), select);
            }
        }

        protected IDictionary<string, object> ReadDoc(
            (ulong collectionId, long docId) docId,
            HashSet<string> select,
            double? score = null
            )
        {
            var streamReader = GetOrCreateDocumentReader(docId.collectionId);
            var docInfo = streamReader.GetDocumentAddress(docId.docId);
            var docMap = streamReader.GetDocumentMap(docInfo.offset, docInfo.length);
            var indexCollectionId = docId.collectionId;
            ulong? sourceCollectionId = null;
            long? sourceDocId = null;
            var doc = new Dictionary<string, object>();

            for (int i = 0; i < docMap.Count; i++)
            {
                var kvp = docMap[i];
                var kInfo = streamReader.GetAddressOfKey(kvp.keyId);
                var key = (string)streamReader.GetKey(kInfo.offset, kInfo.len, kInfo.dataType);

                if (key.StartsWith("___") || select.Contains(key))
                {
                    var vInfo = streamReader.GetAddressOfValue(kvp.valId);
                    var val = streamReader.GetValue(vInfo.offset, vInfo.len, vInfo.dataType);

                    doc[key] = val;

                    if (key == SystemFields.CollectionId)
                    {
                        var docCollectionId = (ulong)val;

                        if (docCollectionId != indexCollectionId)
                        {
                            sourceCollectionId = docCollectionId;
                        }
                    }
                    else if (key == SystemFields.DocumentId)
                    {
                        sourceDocId = (long)val;
                    }
                }
            }

            if (sourceCollectionId.HasValue)
            {
                return ReadDoc((sourceCollectionId.Value, sourceDocId.Value), select, score);
            }

            doc[SystemFields.DocumentId] = docId.docId;

            if (score.HasValue)
                doc[SystemFields.Score] = score;

            return doc;
        }

        private DocumentReader GetOrCreateDocumentReader(ulong collectionId)
        {
            return _streamReaders.GetOrAdd(
                collectionId,
                new DocumentReader(collectionId, SessionFactory)
                );
        }
    }
}
