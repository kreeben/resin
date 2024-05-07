using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
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

        public async Task<IQuery> ParseRequest(HttpRequest request, IEnumerable<string> collections = null)
        {
            var select = request.Query["select"].ToArray();

            if (request.Method == "GET")
            {
                if (collections == null)
                    collections = request.Query["collection"].ToArray();

                var fields = request.Query["field"].ToArray();

                var naturalLanguage = request.Query["q"].ToString();
                bool and = !request.Query.ContainsKey("NOT") || request.Query.ContainsKey("AND");
                bool or = !and && request.Query.ContainsKey("OR");

                return _parser.Parse(collections, naturalLanguage, fields.ToArray(), select, and, or, true);
            }
            else
            {
                var jsonQueryDocument = await DeserializeFromStream(request.Body);

                var query = _parser.Parse(jsonQueryDocument, select, true);

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

        public IQuery ParseFormattedString(string formattedQuery, string[] select)
        {
            var document = JsonConvert.DeserializeObject<IDictionary<string, object>>(
                formattedQuery, new JsonConverter[] { new DictionaryConverter() });

            return ParseDictionary(document, select);
        }

        public IQuery ParseDictionary(IDictionary<string, object> document, string[] select)
        {
            return _parser.Parse(document, select, true);
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

            if (q.AndQuery != null)
            {
                ParseQuery(q.AndQuery, parent);
            }
            if (q.OrQuery != null)
            {
                ParseQuery(q.OrQuery, parent);
            }
            if (q.NotQuery != null)
            {
                ParseQuery(q.NotQuery, parent);
            }
        }

        public void ParseQuery(IQuery query, IDictionary<string, object> result)
        {
            DoParseQuery(query, result);
        }
    }
}
