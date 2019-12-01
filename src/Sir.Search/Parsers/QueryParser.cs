﻿using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace Sir.Search
{
    public class QueryParser
    {
        private readonly SessionFactory _sessionFactory;
        private readonly IStringModel _model;

        public QueryParser(SessionFactory sessionFactory, IStringModel model)
        {
            _sessionFactory = sessionFactory;
            _model = model;
        }

        public Query Parse(string[] collections, string q, string[] fields, bool and, bool or)
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

            return ParseQuery(root);
        }

        public Query ParseQuery(dynamic document)
        {
            Query root = null;
            Query cursor = null;
            string[] parentCollections = null;
            bool and = false;
            bool or = false;
            bool not = false;
            var operation = (JObject)document;

            while (operation != null)
            {
                string[] collections = null;
                string key = null;
                string value = null;
                object next = null;

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

                operation = next as JObject;

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

                        var query = new Query(ParseTerms(collection, key, value, and, or, not), and, or, not);

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

            return root;
        }

        public IList<Term> ParseTerms(string collectionName, string key, string value, bool and, bool or, bool not)
        {
            var collectionId = collectionName.ToHash();
            long keyId;
            var terms = new List<Term>();

            if (_sessionFactory.TryGetKeyId(collectionId, key.ToHash(), out keyId))
            {
                var tokens = _model.Tokenize(value);

                foreach (var term in tokens)
                {
                    terms.Add(new Term(collectionId, keyId, key, term, and, or, not));
                }
            }

            return terms;
        }
    }
}