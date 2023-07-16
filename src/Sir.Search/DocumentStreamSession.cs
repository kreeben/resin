using Sir.Documents;
using Sir.IO;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sir
{
    public class DocumentStreamSession : IDisposable
    {
        private readonly string _directory;
        private readonly IStreamDispatcher _database;
        private readonly IDictionary<ulong, DocumentReader> _documentReaders;

        public DocumentStreamSession(string directory, IStreamDispatcher database) 
        {
            _directory = directory;
            _database = database;
            _documentReaders = new Dictionary<ulong, DocumentReader>();
        }


        public int Count(ulong collectionId)
        {
            var reader = GetOrCreateDocumentReader(collectionId);
            return reader == null ? 0 : reader.DocumentCount();
        }

        public IEnumerable<Document> ReadDocuments(
            ulong collectionId, 
            HashSet<string> select,
            int skip = 0, 
            int take = 0)
        {
            var documentReader = GetOrCreateDocumentReader(collectionId);

            if (documentReader == null)
                yield break;

            var docCount = documentReader.DocumentCount();

            if (take == 0)
                take = docCount;

            var took = 0;
            long docId = skip;

            while (docId < docCount && took++ < take)
            {
                yield return ReadDocument((collectionId, docId++), select, documentReader);
            }

            yield break;
        }

        public IEnumerable<AnalyzedDocument> GetDocumentsAsVectors<T>(
            ulong collectionId,
            HashSet<string> select,
            IModel<T> model,
            bool label,
            int skip = 0,
            int take = 0)
        {
            var documentReader = GetOrCreateDocumentReader(collectionId);

            if (documentReader == null)
                yield break;

            var docCount = documentReader.DocumentCount();

            if (take == 0)
                take = docCount;

            var took = 0;
            long docId = skip;

            while (docId < docCount && took++ < take)
            {
                var columns = new List<VectorNode>();

                foreach (var node in ReadDocumentValuesAsVectors((collectionId, docId), select, documentReader, model, label))
                {
                    columns.Add(node);
                }

                yield return new AnalyzedDocument(columns, docId);

                docId++;
            }
        }

        public IEnumerable<T> ReadDocumentValues<T>(
            ulong collectionId,
            string field,
            int skip = 0,
            int take = 0)
        {
            var documentReader = GetOrCreateDocumentReader(collectionId);

            if (documentReader == null)
                yield break;

            var docCount = documentReader.DocumentCount();

            if (take == 0)
                take = docCount;

            var took = 0;
            long docId = skip;
            var keyId = _database.GetKeyId(_directory, collectionId, field.ToHash());

            while (docId < docCount && took++ < take)
            {
                var value = ReadDocumentValue<T>((collectionId, docId), keyId, documentReader);

                if (value != null)
                    yield return value;

                docId++;
            }
        }

        public IEnumerable<Document> ReadDocuments(
            DocumentReader documentReader,
            HashSet<string> select,
            int skip = 0,
            int take = 0)
        {
            var docCount = documentReader.DocumentCount();

            if (take == 0)
                take = docCount;

            var took = 0;
            long docId = skip;

            while (docId <= docCount && took++ < take)
            {
                yield return ReadDocument((documentReader.CollectionId, docId++), select, documentReader);
            }
        }

        public T ReadDocumentValue<T>(
            (ulong collectionId, long docId) doc,
            long keyId,
            DocumentReader streamReader)
        {
            var docInfo = streamReader.GetDocumentAddress(doc.docId);
            var docMap = streamReader.GetDocumentMap(docInfo.offset, docInfo.length);
            T value = default(T);

            for (int i = 0; i < docMap.Length; i++)
            {
                var kvp = docMap[i];

                if (kvp.keyId == keyId)
                {
                    var vInfo = streamReader.GetAddressOfValue(kvp.valId);

                    value = (T)streamReader.GetValue(vInfo.offset, vInfo.len, vInfo.dataType);

                    break;
                }
            }

            return value;
        }

        public IEnumerable<VectorNode> ReadDocumentValuesAsVectors<T>(
            (ulong collectionId, long docId) doc,
            HashSet<string> select,
            DocumentReader streamReader,
            IModel<T> model,
            bool label,
            SortedList<int, float> embedding = null)
        {
            if (embedding == null)
                embedding = new SortedList<int, float>();

            var docInfo = streamReader.GetDocumentAddress(doc.docId);
            var docMap = streamReader.GetDocumentMap(docInfo.offset, docInfo.length);

            // for each key, create a tree
            for (int i = 0; i < docMap.Length; i++)
            {
                var kvp = docMap[i];
                var kInfo = streamReader.GetAddressOfKey(kvp.keyId);
                var key = (string)streamReader.GetKey(kInfo.offset, kInfo.len, kInfo.dataType);

                if (select.Contains(key))
                {
                    var tree = new VectorNode(keyId: kvp.keyId);
                    var vInfo = streamReader.GetAddressOfValue(kvp.valId);

                    foreach (var vector in streamReader.GetValueConvertedToVectors<T>(vInfo.offset, vInfo.len, vInfo.dataType, value => model.CreateEmbedding(value, label, embedding)))
                    {
                        tree.AddIfUnique(new VectorNode(vector, docId:doc.docId, keyId:kvp.keyId), model);
                    }

                    yield return tree;
                }
            }
        }

        public Document ReadDocument(
            (ulong collectionId, long docId) docId,
            HashSet<string> select,
            double? score = null)
        {
            var reader = GetOrCreateDocumentReader(docId.collectionId);

            if (reader == null)
                return null;

            return ReadDocument(docId, select, reader, score);
        }

        public Document ReadDocument(
            (ulong collectionId, long docId) doc,
            HashSet<string> select,
            DocumentReader streamReader,
            double? score = null)
        {
            var docInfo = streamReader.GetDocumentAddress(doc.docId);
            var docMap = streamReader.GetDocumentMap(docInfo.offset, docInfo.length);
            var fields = new List<Field>();

            for (int i = 0; i < docMap.Length; i++)
            {
                var kvp = docMap[i];
                var kInfo = streamReader.GetAddressOfKey(kvp.keyId);
                var key = (string)streamReader.GetKey(kInfo.offset, kInfo.len, kInfo.dataType);

                if (select.Contains(key))
                {
                    var vInfo = streamReader.GetAddressOfValue(kvp.valId);
                    var val = streamReader.GetValue(vInfo.offset, vInfo.len, vInfo.dataType);

                    fields.Add(new Field(key, val, kvp.keyId));
                }
            }

            return new Document(fields, collectionId:doc.collectionId, documentId:doc.docId, score:(score.HasValue ? score.Value : 0));
        }

        private DocumentReader GetOrCreateDocumentReader(ulong collectionId)
        {
            if (!File.Exists(Path.Combine(_directory, string.Format("{0}.val", collectionId))))
                return null;

            DocumentReader reader;

            if (!_documentReaders.TryGetValue(collectionId, out reader))
            {
                reader = new DocumentReader(_directory, collectionId, _database);
                _documentReaders.Add(collectionId, reader);
            }

            return reader;
        }

        public virtual void Dispose()
        {
            foreach (var reader in _documentReaders.Values)
            {
                reader.Dispose();
            }
        }
    }
}