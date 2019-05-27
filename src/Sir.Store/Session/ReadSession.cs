using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir.Store
{
    /// <summary>
    /// Read session targeting a single collection.
    /// </summary>
    public class ReadSession : DocumentSession, ILogger
    {
        private readonly DocIndexReader _docIx;
        private readonly DocMapReader _docs;
        private readonly ValueIndexReader _keyIx;
        private readonly ValueIndexReader _valIx;
        private readonly ValueReader _keyReader;
        private readonly ValueReader _valReader;
        private readonly IConfigurationProvider _config;

        public ReadSession(string collectionName,
            ulong collectionId,
            SessionFactory sessionFactory, 
            IConfigurationProvider config) 
            : base(collectionName, collectionId, sessionFactory)
        {
            ValueStream = sessionFactory.CreateReadStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.val", CollectionId)));
            KeyStream = sessionFactory.CreateReadStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.key", CollectionId)));
            DocStream = sessionFactory.CreateReadStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.docs", CollectionId)));
            ValueIndexStream = sessionFactory.CreateReadStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.vix", CollectionId)));
            KeyIndexStream = sessionFactory.CreateReadStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.kix", CollectionId)));
            DocIndexStream = sessionFactory.CreateReadStream(Path.Combine(sessionFactory.Dir, string.Format("{0}.dix", CollectionId)));

            _docIx = new DocIndexReader(DocIndexStream);
            _docs = new DocMapReader(DocStream);
            _keyIx = new ValueIndexReader(KeyIndexStream);
            _valIx = new ValueIndexReader(ValueIndexStream);
            _keyReader = new ValueReader(KeyStream);
            _valReader = new ValueReader(ValueStream);
            _config = config;
        }

        public ReadResult Read(Query query)
        {
            if (SessionFactory.CollectionExists(query.Collection))
            {
                var result = MapReduce(query);

                if (result != null)
                {
                    var docs = ReadDocs(result.SortedDocuments);

                    return new ReadResult { Total = result.Total, Docs = docs };
                }
            }

            this.Log("found nothing for query {0}", query);

            return new ReadResult { Total = 0, Docs = new IDictionary<string, object>[0] };
        }

        public IEnumerable<long> ReadIds(Query query)
        {
            if (SessionFactory.CollectionExists(query.Collection))
            {
                var result = MapReduce(query);

                if (result == null)
                {
                    this.Log("found nothing for query {0}", query);

                    return new long[0];
                }

                return result.Documents.Keys;
            }

            return new long[0];
        }

        /// <summary>
        /// Find each query term's corresponding index node and postings list and perform "AND", "OR" or "NOT" set operations on them.
        /// </summary>
        /// <param name="query"></param>
        private ScoredResult MapReduce(Query query)
        {
            Map(query);

            var timer = Stopwatch.StartNew();

            using (var postingsStream = SessionFactory.CreateReadStream(Path.Combine(SessionFactory.Dir, $"{CollectionId}.pos")))
            {
                var result = new PostingsReader(postingsStream).Reduce(query.ToList(), query.Skip, query.Take);

                this.Log("reducing {0} into {1} docs took {2}", query, result.Documents.Count, timer.Elapsed);

                return result;
            }
        }

        /// <summary>
        /// Map query terms to index IDs.
        /// </summary>
        /// <param name="query">An un-mapped query</param>
        public void Map(Query query)
        {
            var timer = Stopwatch.StartNew();

            var clauses = query.ToList();

            //Parallel.ForEach(clauses, q =>
            foreach (var q in clauses)
            {
                var cursor = q;

                while (cursor != null)
                {
                    Hit hit = null;

                    var indexReader = cursor.Term.KeyId.HasValue ?
                        CreateIndexReader(cursor.Term.KeyId.Value) :
                        CreateIndexReader(cursor.Term.KeyHash);

                    if (indexReader != null)
                    {
                        var termVector = cursor.Term.AsVector();

                        hit = indexReader.ClosestMatch(termVector, Similarity.Term);
                    }

                    if (hit != null && hit.Score > 0)
                    {
                        cursor.Score = hit.Score;

                        if (hit.Node.PostingsOffsets == null)
                        {
                            cursor.PostingsOffsets.Add(hit.Node.PostingsOffset);
                        }
                        else
                        {
                            foreach (var offs in hit.Node.PostingsOffsets)
                            {
                                if (offs < 0)
                                {
                                    throw new DataMisalignedException();
                                }

                                cursor.PostingsOffsets.Add(offs);
                            }
                        }
                    }

                    cursor = cursor.Then;
                }
            }//);

            this.Log("mapping {0} took {1}", query, timer.Elapsed);
        }

        public NodeReader CreateIndexReader(long keyId)
        {
            var ixFileName = Path.Combine(SessionFactory.Dir, string.Format("{0}.{1}.ix", CollectionId, keyId));

            if (!File.Exists(ixFileName))
                return null;

            return new NodeReader(CollectionId, keyId, SessionFactory, _config);
        }

        public NodeReader CreateIndexReader(ulong keyHash)
        {
            long keyId;
            if (!SessionFactory.TryGetKeyId(CollectionId, keyHash, out keyId))
            {
                return null;
            }

            return CreateIndexReader(keyId);
        }

        public IList<IDictionary<string, object>> ReadDocs(IEnumerable<KeyValuePair<long, float>> docs)
        {
            var result = new List<IDictionary<string, object>>();

            foreach (var d in docs)
            {
                var docInfo = _docIx.Read(d.Key);

                if (docInfo.offset < 0)
                {
                    continue;
                }

                var docMap = _docs.Read(docInfo.offset, docInfo.length);
                var doc = new Dictionary<string, object>();

                for (int i = 0; i < docMap.Count; i++)
                {
                    var kvp = docMap[i];
                    var kInfo = _keyIx.Read(kvp.keyId);
                    var vInfo = _valIx.Read(kvp.valId);
                    var key = _keyReader.Read(kInfo.offset, kInfo.len, kInfo.dataType);
                    var val = _valReader.Read(vInfo.offset, vInfo.len, vInfo.dataType);

                    doc[key.ToString()] = val;
                }

                doc["___docid"] = d.Key;
                doc["___score"] = d.Value;

                result.Add(doc);
            }

            return result;
        }

        public IList<IDictionary<string, object>> ReadDocs(IEnumerable<long> docs)
        {
            var result = new List<IDictionary<string, object>>();

            foreach (var docId in docs)
            {
                var docInfo = _docIx.Read(docId);

                if (docInfo.offset < 0)
                {
                    continue;
                }

                var docMap = _docs.Read(docInfo.offset, docInfo.length);
                var doc = new Dictionary<string, object>();

                for (int i = 0; i < docMap.Count; i++)
                {
                    var kvp = docMap[i];
                    var kInfo = _keyIx.Read(kvp.keyId);
                    var vInfo = _valIx.Read(kvp.valId);
                    var key = _keyReader.Read(kInfo.offset, kInfo.len, kInfo.dataType);
                    var val = _valReader.Read(vInfo.offset, vInfo.len, vInfo.dataType);

                    doc[key.ToString()] = val;
                }

                doc["___docid"] = docId;
                doc["___score"] = 1f;

                result.Add(doc);
            }

            return result;
        }
    }
}
