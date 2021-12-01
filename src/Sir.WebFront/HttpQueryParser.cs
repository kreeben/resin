﻿using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Sir.Search;
using Sir.VectorSpace;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Sir.HttpServer
{
    /// <summary>
    /// Parse http request query or body into a <see cref="Query"/>.
    /// </summary>
    public class HttpQueryParser
    {
        private readonly QueryParser<string> _parser;

        public HttpQueryParser(QueryParser<string> parser)
        {
            _parser = parser;
        }

        public async Task<Query> ParseRequest(HttpRequest request, IEnumerable<string> collections = null, IEnumerable<string> fields = null, IEnumerable<string> select = null)
        {
            if (select == null)
                select = request.Query["select"].ToArray();

            if (request.Method == "GET")
            {
                if (collections == null)
                    collections = request.Query["collection"].ToArray();

                if (fields == null)
                    fields = request.Query["field"].ToArray();

                var naturalLanguage = request.Query["q"].ToString();
                bool and = request.Query.ContainsKey("AND");
                bool or = !and && request.Query.ContainsKey("OR");

                return _parser.Parse(collections, naturalLanguage, fields.ToArray(), select, and, or);
            }
            else
            {
                var jsonQueryDocument = await DeserializeFromStream(request.Body);

                var query = _parser.Parse(jsonQueryDocument, select);

                return query;
            }
        }

        public static async Task<dynamic> DeserializeFromStream(Stream stream)
        {
            using (var sr = new StreamReader(stream))
            {
                var json = await sr.ReadToEndAsync();
                return JsonConvert.DeserializeObject<ExpandoObject>(json);
            }
        }

        public Query ParseFormattedString(string formattedQuery, string[] select)
        {
            var document = JsonConvert.DeserializeObject<IDictionary<string, object>>(
                formattedQuery, new JsonConverter[] { new DictionaryConverter() });

            return ParseDictionary(document, select);
        }

        public Query ParseDictionary(IDictionary<string, object> document, string[] select)
        {
            return _parser.Parse(document, select);
        }

        private void DoParseQuery(IQuery query, IDictionary<string, object> result)
        {
            if (result == null)
                return;

            var parent = result;
            var q = (Query)query;

            foreach (var term in q.Terms)
            {
                var termdic = new Dictionary<string, object>();

                termdic.Add("collection", term.CollectionId);
                termdic.Add(term.Key, term.Vector.Label);

                if (term.IsIntersection)
                {
                    parent.Add("and", termdic);
                }
                else if (term.IsUnion)
                {
                    parent.Add("or", termdic);
                }
                else
                {
                    parent.Add("not", termdic);
                }

                parent = termdic;
            }

            if (q.And != null)
            {
                ParseQuery(q.And, parent);
            }
            if (q.Or != null)
            {
                ParseQuery(q.Or, parent);
            }
            if (q.Not != null)
            {
                ParseQuery(q.Not, parent);
            }
        }

        public void ParseQuery(IQuery query, IDictionary<string, object> result)
        {
            DoParseQuery(query, result);
        }
    }
}
