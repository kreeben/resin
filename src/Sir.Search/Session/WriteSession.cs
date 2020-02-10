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

        public long EnsureKeyExists(string key)
        {
            return _streamWriter.EnsureKeyExists(key);
        }

        /// <summary>
        /// Fields prefixed with "_" will not be indexed.
        /// Fields prefixed with "__" will not be stored.
        /// </summary>
        /// <returns>Document ID</returns>
        public long Write(IDictionary<string, object> document, HashSet<string> storedFieldNames)
        {
            var docMap = new List<(long keyId, long valId)>();

            Write("created", DateTime.Now.ToBinary(), docMap);
            Write("collection", _collectionId, docMap);

            foreach (var key in document.Keys)
            {
                var val = document[key];

                if ((val == null) || (key != "collectionid" && !storedFieldNames.Contains(key)))
                {
                    continue;
                }

                Write(key, val, docMap);
            }

            var docMeta = _streamWriter.PutDocumentMap(docMap);
            var docId = _streamWriter.GetNextDocId();

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