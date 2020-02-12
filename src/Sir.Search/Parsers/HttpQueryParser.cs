using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Sir.Search
{
    /// <summary>
    /// Parse text from a http request message into a query.
    /// </summary>
    public class HttpQueryParser
    {
        private readonly QueryParser _parser;

        public HttpQueryParser(QueryParser parser)
        {
            _parser = parser;
        }

        public async Task<IQuery> ParseRequest(HttpRequest request)
        {
            if (request.Method == "GET")
            {
                if (!request.Query.ContainsKey("collection"))
                {
                    throw new InvalidOperationException("collection missing from query string");
                }

                string[] collections = request.Query["collection"].ToArray()
                    .SelectMany(x=>x.Split(',', StringSplitOptions.RemoveEmptyEntries))
                    .ToArray();
                var naturalLanguage = request.Query["q"].ToString();
                string[] fields = request.Query["field"].ToArray()
                    .SelectMany(x => x.Split(',', StringSplitOptions.RemoveEmptyEntries))
                    .ToArray();
                bool and = request.Query.ContainsKey("AND");
                bool or = !and && request.Query.ContainsKey("OR");

                return _parser.Parse(collections, naturalLanguage, fields, and, or);
            }
            else
            {
                var jsonDocument = await DeserializeFromStream(request.Body);

                var query = _parser.Parse(jsonDocument);

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

        public IQuery ParseFormattedString(string formattedQuery)
        {
            var document = JsonConvert.DeserializeObject<IDictionary<string, object>>(
                formattedQuery, new JsonConverter[] { new DictionaryConverter() });

            return ParseDictionary(document);
        }

        public IQuery ParseDictionary(IDictionary<string, object> document)
        {
            return _parser.Parse(document);
        }

        private void DoParseQuery(Query query, IDictionary<string, object> result)
        {
            if (result == null)
                return;

            var parent = result;
            var q = (Query)query;

            foreach (var term in q.Terms)
            {
                var termdic = new Dictionary<string, object>();

                termdic.Add("collection", term.CollectionId);
                termdic.Add(term.Key, term.Vector.Data.ToString());

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
            if (query is Query)
                DoParseQuery((Query)query, result);
            else
                DoParseQuery(((Join)query).Query, result);
        }
    }

    /// <summary>
    /// https://stackoverflow.com/questions/6416017/json-net-deserializing-nested-dictionaries
    /// </summary>
    public class DictionaryConverter : CustomCreationConverter<IDictionary<string, object>>
    {
        public override IDictionary<string, object> Create(Type objectType)
        {
            return new Dictionary<string, object>();
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(object) || base.CanConvert(objectType);
        }

        public override object ReadJson(
            JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.StartObject
                || reader.TokenType == JsonToken.Null)
                return base.ReadJson(reader, objectType, existingValue, serializer);

            return serializer.Deserialize(reader);
        }
    }
}
