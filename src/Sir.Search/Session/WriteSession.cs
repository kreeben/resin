﻿using Sir.Document;
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

        /// <summary>
        /// Fields prefixed with "_" will not be indexed.
        /// Fields prefixed with "__" will not be stored.
        /// </summary>
        /// <returns>Document ID</returns>
        public void Write(IDictionary<string, object> document, HashSet<string> storedFieldNames)
        {
            document["created"] = DateTime.Now.ToBinary();
            document["collectionid"] = _collectionId;

            var docMap = new List<(long keyId, long valId)>();
            var docId = _streamWriter.GetNextDocId();

            foreach (var key in document.Keys)
            {
                var keyId = _streamWriter.EnsureKeyExists(key);

                if (key != "collectionid" && !storedFieldNames.Contains(key))
                {
                    continue;
                }

                var val = document[key];

                if (val == null)
                {
                    continue;
                }

                byte dataType;

                // store k/v
                var kvmap = _streamWriter.Put(keyId, val, out dataType);

                // store refs to k/v pair
                docMap.Add(kvmap);
            }

            var docMeta = _streamWriter.PutDocumentMap(docMap);

            _streamWriter.PutDocumentAddress(docId, docMeta.offset, docMeta.length);

            document["___docid"] = docId;
        }
    }
}