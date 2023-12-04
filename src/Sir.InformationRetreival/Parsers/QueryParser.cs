using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.KeyValue;
using System.Collections.Generic;

namespace Sir
{
    public class QueryParser<T>
    {
        private readonly KeyValueWriter _kvwriter;
        private readonly IModel<T> _model;
        private readonly ILogger _logger;
        private readonly string _directory;
        private readonly SortedList<int, float> _embedding;

        public QueryParser(string directory, KeyValueWriter kvwriter, IModel<T> model, SortedList<int, float> embedding = null, ILogger logger = null)
        {
            _kvwriter = kvwriter;
            _model = model;
            _logger = logger;
            _directory = directory;
            _embedding = embedding ?? new SortedList<int, float>();
        }

        public IQuery Parse(
            ulong collectionId,
            T query,
            string field,
            string select,
            bool and,
            bool or,
            bool label)
        {
            var terms = CreateTerms(
                collectionId,
                field,
                query,
                and,
                or,
                !and && !or,
                label);

            return new Query(terms, new string[] { select }, and, or, !and && !or);
        }

        public IQuery Parse(
            string collection,
            T query,
            string field,
            string select,
            bool and,
            bool or,
            bool label)
        {
            var terms = CreateTerms(
                collection,
                field,
                query,
                and,
                or,
                !and && !or,
                label);

            return new Query(terms, new string[] { select }, and, or, !and && !or);
        }

        public IQuery Parse(
            IEnumerable<string> collections,
            T q, 
            string[] fields, 
            IEnumerable<string> select, 
            bool and, 
            bool or,
            bool label)
        {
            var root = new Dictionary<string, object>();
            var cursor = root;

            foreach (var collection in collections)
            {
                var query = new Dictionary<string, object>
                {
                    {"collection", collection }
                };

                if (and)
                {
                    cursor["and"] = query;
                }
                else if (or)
                {
                    cursor["or"] = query;
                }
                else
                {
                    cursor["not"] = query;
                }

                if (fields.Length == 1)
                {
                    query[fields[0]] = q;
                }
                else
                {
                    for (int i = 0; i < fields.Length; i++)
                    {
                        query[fields[i]] = q;

                        if (i < fields.Length - 1)
                        {
                            var next = new Dictionary<string, object>
                            {
                                {"collection", collection }
                            };

                            if (and)
                            {
                                query["and"] = next;
                            }
                            else if (or)
                            {
                                query["or"] = next;
                            }
                            else
                            {
                                query["not"] = next;
                            }

                            query = next;
                        }
                    }
                }

                cursor = query;
            }

            if (_logger != null && _logger.IsEnabled(LogLevel.Trace))
            {
                var queryLog = JsonConvert.SerializeObject(root);
                _logger.LogTrace($"incoming query: {queryLog}");
            }

            return Parse(root, select, label);
        }

        public IQuery Parse(dynamic document, IEnumerable<string> select, bool label)
        {
            Query root = null;
            Query cursor = null;
            string[] parentCollections = null;
            bool and = false;
            bool or = false;
            bool not = false;
            var operation = document;

            while (operation != null)
            {
                string[] collections = null;
                var kvps = new List<(string key, T value)>();
                dynamic next = null;

                foreach (var kvp in operation)
                {
                    if (kvp.Key == "collection")
                    {
                        collections = ((string)kvp.Value)
                            .Split(',', System.StringSplitOptions.RemoveEmptyEntries);

                        parentCollections = collections;

                    }
                    else if (kvp.Key == "and")
                    {
                        and = true;
                        next = kvp.Value;
                    }
                    else if (kvp.Key == "or")
                    {
                        or = true;
                        next = kvp.Value;
                    }
                    else if (kvp.Key == "not")
                    {
                        not = true;
                        next = kvp.Value;
                    }
                    else
                    {
                        var keys = ((string)kvp.Key).Split(',', System.StringSplitOptions.RemoveEmptyEntries);

                        foreach (var k in keys)
                            kvps.Add((k, kvp.Value));
                    }
                }

                operation = next;

                if (kvps.Count == 0)
                {
                    continue;
                }
                else
                {
                    foreach (var collection in collections ?? parentCollections)
                    {
                        foreach (var kvp in kvps)
                        {
                            var terms = CreateTerms(collection, kvp.key, kvp.value, and, or, not, label);

                            if (terms.Count == 0)
                            {
                                continue;
                            }

                            var query = new Query(terms, select, and, or, not);

                            if (root == null)
                            {
                                root = cursor = query;
                            }
                            else
                            {
                                cursor.OrQuery = query;

                                cursor = query;
                            }
                        }
                    }
                }
            }
                
            return root;
        }

        private IList<Term> CreateTerms(string collectionName, string key, T value, bool and, bool or, bool not, bool label)
        {
            var collectionId = collectionName.ToHash();
            return CreateTerms(collectionId, key, value, and, or, not, label);
        }

        private IList<Term> CreateTerms(ulong collectionId, string key, T value, bool and, bool or, bool not, bool label)
        {
            long keyId;
            var terms = new List<Term>();

            if (_kvwriter.TryGetKeyId(_directory, collectionId, key.ToHash(), out keyId))
            {
                var tokens = _model.CreateEmbedding(value, label, _embedding);

                foreach (var term in tokens)
                {
                    terms.Add(new Term(_directory, collectionId, keyId, key, term, and, or, not));
                }
            }

            return terms;
        }
    }
}