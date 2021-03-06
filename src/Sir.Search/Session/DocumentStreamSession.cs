﻿using Sir.Documents;
using Sir.VectorSpace;
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

        public IEnumerable<Document> ReadDocuments(
            ulong collectionId, 
            HashSet<string> select,
            int skip = 0, 
            int take = 0)
        {
            var documentReader = GetOrCreateDocumentReader(collectionId);
            var docCount = documentReader.DocumentCount();

            if (take == 0)
                take = docCount;

            var took = 0;
            long docId = skip;

            while (docId < docCount && took++ < take)
            {
                yield return ReadDocument((collectionId, docId++), select, documentReader);
            }
        }

        public IEnumerable<AnalyzedDocument> ReadDocumentVectors<T>(
            ulong collectionId,
            HashSet<string> select,
            IModel<T> model,
            int skip = 0,
            int take = 0)
        {
            var documentReader = GetOrCreateDocumentReader(collectionId);
            var docCount = documentReader.DocumentCount();

            if (take == 0)
                take = docCount;

            var took = 0;
            long docId = skip;

            while (docId < docCount && took++ < take)
            {
                var columns = new List<VectorNode>();

                foreach (var node in ReadDocumentVectors((collectionId, docId), select, documentReader, model))
                {
                    columns.Add(node);
                }

                yield return new AnalyzedDocument(columns);

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

        public Document ReadDocument(
            (ulong collectionId, long docId) doc,
            HashSet<string> select,
            DocumentReader streamReader,
            double? score = null
            )
        {
            var docInfo = streamReader.GetDocumentAddress(doc.docId);
            var docMap = streamReader.GetDocumentMap(docInfo.offset, docInfo.length);
            var fields = new List<Field>();

            for (int i = 0; i < docMap.Count; i++)
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

            return new Document(fields, doc.docId, score.HasValue ? score.Value : 0);
        }

        public IEnumerable<VectorNode> ReadDocumentVectors<T>(
            (ulong collectionId, long docId) doc,
            HashSet<string> select,
            DocumentReader streamReader,
            IModel<T> model)
        {
            var docInfo = streamReader.GetDocumentAddress(doc.docId);
            var docMap = streamReader.GetDocumentMap(docInfo.offset, docInfo.length);

            // for each key, create a tree
            for (int i = 0; i < docMap.Count; i++)
            {
                var kvp = docMap[i];
                var kInfo = streamReader.GetAddressOfKey(kvp.keyId);
                var key = (string)streamReader.GetKey(kInfo.offset, kInfo.len, kInfo.dataType);
                var tree = new VectorNode(keyId:kvp.keyId);

                if (select.Contains(key))
                {
                    var vInfo = streamReader.GetAddressOfValue(kvp.valId);

                    foreach (var vector in streamReader.GetVectors<T>(vInfo.offset, vInfo.len, vInfo.dataType, value => model.Tokenize(value)))
                    {
                        tree.AddIfUnique(new VectorNode(vector, docId:doc.docId, keyId:kvp.keyId), model);
                    }

                    yield return tree;
                }
            }
        }

        public Document ReadDoc(
            (ulong collectionId, long docId) docId,
            HashSet<string> select,
            double? score = null)
        {
            var streamReader = GetOrCreateDocumentReader(docId.collectionId);

            return ReadDocument(docId, select, streamReader, score);
        }

        private DocumentReader GetOrCreateDocumentReader(ulong collectionId)
        {
            return _streamReaders.GetOrAdd(
                collectionId,
                key => new DocumentReader(key, SessionFactory));
        }
    }
}