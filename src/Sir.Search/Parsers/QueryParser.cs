using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace Sir.Search
{
    public class QueryParser
    {
        private readonly SessionFactory _sessionFactory;
        private readonly IStringModel _model;
        private readonly ILogger<QueryParser> _log;

        public QueryParser(SessionFactory sessionFactory, IStringModel model)
        {
            _sessionFactory = sessionFactory;
            _model = model;
            _log = _sessionFactory.GetLogger<QueryParser>();
        }

        public IQuery Parse(
            string[] collections,
            string q, 
            string[] fields, 
            IEnumerable<string> select, 
            bool and, 
            bool or)
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

            _log.LogDebug(JsonConvert.SerializeObject(root));

            return Parse(root, select);
        }

        public IQuery Parse(dynamic document, IEnumerable<string> select)
        {
            //if (((IDictionary<string,object>)document).ContainsKey("join"))
            //{
            //    return ParseJoin(document);
            //}

            return ParseQuery(document, select);
        }

        public Query ParseQuery(dynamic document, IEnumerable<string> select)
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
                string key = null;
                string value = null;
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
                        key = kvp.Key;
                        value = (string)kvp.Value;
                    }
                }

                operation = next;

                if (value == null)
                {
                    continue;
                }
                else
                {
                    foreach (var collection in collections ?? parentCollections)
                    {
                        var terms = ParseTerms(collection, key, value, and, or, not);

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
                            cursor.Or = query;

                            cursor = query;
                        }
                    }
                }
            }

            _log.LogDebug(JsonConvert.SerializeObject(root));

            return root;
        }

        //public Join ParseJoin(dynamic document)
        //{
        //    string[] joinInfo = ((string)((IDictionary<string,object>)document)["join"])
        //        .Split(',', System.StringSplitOptions.RemoveEmptyEntries);

        //    string joinCollection = joinInfo[0];
        //    string primaryKey = joinInfo[1];
        //    var query = ParseQuery(document.query);
        //    var root = new Join(query, joinCollection, primaryKey);

        //    _log.LogDebug(JsonConvert.SerializeObject(root));

        //    return root;
        //}

        public IList<Term> ParseTerms(string collectionName, string key, string value, bool and, bool or, bool not)
        {
            var collectionId = collectionName.ToHash();
            long keyId;
            var terms = new List<Term>();

            if (_sessionFactory.TryGetKeyId(collectionId, key.ToHash(), out keyId))
            {
                var tokens = _model.Tokenize(value.ToCharArray());

                foreach (var term in tokens)
                {
                    terms.Add(new Term(collectionId, keyId, key, term, and, or, not));
                }
            }

            return terms;
        }
    }
}