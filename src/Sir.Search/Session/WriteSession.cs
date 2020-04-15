using Sir.Document;
using System;
using System.Collections.Generic;

namespace Sir.Search
{
    /// <summary>
    /// Write session targeting a single collection.
    /// </summary>
    public class WriteSession : IDisposable
    {
        private readonly ulong _collectionId;
        private readonly DocumentWriter _streamWriter;

        public WriteSession(
            ulong collectionId,
            DocumentWriter streamWriter)
        {
            _collectionId = collectionId;
            _streamWriter = streamWriter;
        }

        public void Dispose()
        {
            _streamWriter.Dispose();
        }

        public long Write(IDictionary<string, object> document, HashSet<string> storedFieldNames)
        {
            var docMap = new List<(long keyId, long valId)>();

            foreach (var key in document.Keys)
            {
                var val = document[key];

                if (val != null && storedFieldNames.Contains(key))
                {
                    Write(key, val, docMap);
                }
                else
                {
                    continue;
                }
            }

            object collectionId;

            if (!document.TryGetValue(SystemFields.CollectionId, out collectionId))
            {
                collectionId = _collectionId;
            }

            object sourceDocId;

            if (document.TryGetValue(SystemFields.DocumentId, out sourceDocId))
            {
                Write(SystemFields.DocumentId, (long)sourceDocId, docMap);
            }

            Write(SystemFields.Created, DateTime.Now.ToBinary(), docMap);
            Write(SystemFields.CollectionId, collectionId, docMap);

            var docMeta = _streamWriter.PutDocumentMap(docMap);
            var docId = _streamWriter.IncrementDocId();

            _streamWriter.PutDocumentAddress(docId, docMeta.offset, docMeta.length);
            
            return docId;
        }

        private void Write(string key, object val, IList<(long, long)> docMap)
        {
            var keyId = _streamWriter.EnsureKeyExists(key);

            // store k/v
            var kvmap = _streamWriter.Put(keyId, val, out _);

            // store refs to k/v pair
            docMap.Add(kvmap);
        }
    }
}