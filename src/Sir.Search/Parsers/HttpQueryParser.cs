using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

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

        public Query ParseRequest(HttpRequest request)
        {
            if (request.Method == "GET")
            {
                if (!request.Query.ContainsKey("collection"))
                {
                    throw new InvalidOperationException("collectionId missing from query string");
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
                var jsonQuery = DeserializeFromStream(request.Body);

                return ParseDictionary(jsonQuery);
            }
        }

        public static Dictionary<string, object> DeserializeFromStream(Stream stream)
        {
            var serializer = new JsonSerializer();

            using (var sr = new StreamReader(stream))
            using (var jsonTextReader = new JsonTextReader(sr))
            {
                return serializer.Deserialize<Dictionary<string, object>>(jsonTextReader);
            }
        }

        public Query ParseFormattedString(string formattedQuery)
        {
            var document = JsonConvert.DeserializeObject<IDictionary<string, object>>(
                formattedQuery, new JsonConverter[] { new DictionaryConverter() });

            return ParseDictionary(document);
        }

        public Query ParseDictionary(IDictionary<string, object> document)
        {
            return _parser.ParseQuery(document);
        }

        public void ParseQuery(Query query, IDictionary<string, object> result)
        {
            if (result == null)
                return;

            var parent = result;

            foreach (var term in query.Terms)
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

            if (query.And != null)
            {
                ParseQuery(query.And, parent);
            }
            if (query.Or != null)
            {
                ParseQuery(query.Or, parent);
            }
            if (query.Not != null)
            {
                ParseQuery(query.Not, parent);
            }
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
