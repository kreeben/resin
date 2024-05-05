using Sir.Documents;
using System;
using System.Collections.Generic;

namespace Sir
{
    /// <summary>
    /// Write session targeting a single collection.
    /// </summary>
    public class WriteSession : IDisposable
    {
        private readonly DocInfoWriter _documentWriter;

        public WriteSession(
            DocInfoWriter documentWriter)
        {
            _documentWriter = documentWriter;
        }

        public void Put(Document document)
        {
            var docMap = new List<(long keyId, long valId)>();

            document.Id = _documentWriter.IncrementDocId();

            foreach (var field in document.Fields)
            {
                field.DocumentId = document.Id;

                if (field.Value != null)
                {
                    Write(field, docMap);
                }
            }

            var docMeta = _documentWriter.PutDocumentMap(docMap);

            _documentWriter.PutDocumentAddress(document.Id, docMeta.offset, docMeta.length);
        }

        private void Write(Field field, IList<(long, long)> docMap)
        {
            field.KeyId = EnsureKeyExists(field.Name);

            Write(field.KeyId, field.Value, docMap);
        }

        private void Write(long keyId, object val, IList<(long, long)> docMap)
        {
            // store value
            var kvmap = _documentWriter.PutValue(keyId, val, out _);

            // store refs to k/v pair
            docMap.Add(kvmap);
        }

        public long EnsureKeyExists(string key)
        {
            return _documentWriter.EnsureKeyExists(key);
        }

        public long EnsureKeyExistsSafely(string key)
        {
            return _documentWriter.EnsureKeyExistsSafely(key);
        }

        public void Dispose()
        {
            _documentWriter.Dispose();
        }
    }
}