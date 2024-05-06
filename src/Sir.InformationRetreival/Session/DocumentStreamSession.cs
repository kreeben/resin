using Sir.Documents;
using Sir.IO;
using Sir.KeyValue;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sir
{
    public class DocumentStreamSession : IDisposable
    {
        private readonly string _directory;
        private readonly KeyValueWriter _kvwriter;
        private readonly IDictionary<ulong, DocumentInfoReader> _documentReaders;

        public DocumentStreamSession(string directory, KeyValueWriter kvwriter) 
        {
            _directory = directory;
            _kvwriter = kvwriter;
            _documentReaders = new Dictionary<ulong, DocumentInfoReader>();
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
                yield return DocumentReader.Read(docId++, select, documentReader);
            }
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

                yield return new AnalyzedDocument(columns);

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
            var keyId = _kvwriter.GetKeyId(_directory, collectionId, field.ToHash());

            while (docId < docCount && took++ < take)
            {
                var value = ReadDocumentValue<T>(docId, keyId, documentReader);

                if (value != null)
                    yield return value;

                docId++;
            }
        }

        public IEnumerable<Document> ReadDocuments(
            DocumentInfoReader documentReader,
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
                yield return DocumentReader.Read(docId++, select, documentReader);
            }
        }

        public T ReadDocumentValue<T>(
            long docId,
            long keyId,
            DocumentInfoReader documentReader)
        {
            var docInfo = documentReader.GetDocumentAddress(docId);
            var docMap = documentReader.GetDocumentMap(docInfo.offset, docInfo.length);
            T value = default(T);

            for (int i = 0; i < docMap.Length; i++)
            {
                var kvp = docMap[i];

                if (kvp.keyId == keyId)
                {
                    var vInfo = documentReader.GetAddressOfValue(kvp.valId);

                    value = (T)documentReader.GetValue(vInfo.offset, vInfo.len, vInfo.dataType);

                    break;
                }
            }

            return value;
        }

        public IEnumerable<VectorNode> ReadDocumentValuesAsVectors<T>(
            (ulong collectionId, long docId) doc,
            HashSet<string> select,
            DocumentInfoReader documentReader,
            IModel<T> model,
            bool label,
            SortedList<int, float> embedding = null)
        {
            if (embedding == null)
                embedding = new SortedList<int, float>();

            var docInfo = documentReader.GetDocumentAddress(doc.docId);
            var docMap = documentReader.GetDocumentMap(docInfo.offset, docInfo.length);

            // for each key, create a tree
            for (int i = 0; i < docMap.Length; i++)
            {
                var kvp = docMap[i];
                var kInfo = documentReader.GetAddressOfKey(kvp.keyId);
                var key = (string)documentReader.GetKey(kInfo.offset, kInfo.len, kInfo.dataType);
                var tree = new VectorNode(keyId:kvp.keyId);

                if (select.Contains(key))
                {
                    var vInfo = documentReader.GetAddressOfValue(kvp.valId);

                    foreach (var vector in documentReader.GetValueConvertedToVectors<T>(vInfo.offset, vInfo.len, vInfo.dataType, value => model.CreateEmbedding(value, label, embedding)))
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

            return DocumentReader.Read(docId.docId, select, reader, score);
        }

        private DocumentInfoReader GetOrCreateDocumentReader(ulong collectionId)
        {
            if (!File.Exists(Path.Combine(_directory, string.Format("{0}.val", collectionId))))
                return null;

            DocumentInfoReader reader;

            if (!_documentReaders.TryGetValue(collectionId, out reader))
            {
                reader = new DocumentInfoReader(_directory, collectionId);
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

            _kvwriter.Dispose();
        }
    }
}