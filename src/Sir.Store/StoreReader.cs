using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;

namespace Sir.Store
{
    /// <summary>
    /// Query a collection.
    /// </summary>
    public class StoreReader : IReader, ILogger
    {
        public string ContentType => "application/json";

        private readonly SessionFactory _sessionFactory;
        private readonly HttpQueryParser _httpQueryParser;
        private readonly HttpBowQueryParser _httpBowQueryParser;
        private readonly ITokenizer _tokenizer;

        public StoreReader(
            SessionFactory sessionFactory, 
            HttpQueryParser httpQueryParser, 
            HttpBowQueryParser httpDocumentQueryParser, 
            ITokenizer tokenizer)
        {
            _sessionFactory = sessionFactory;
            _httpQueryParser = httpQueryParser;
            _tokenizer = tokenizer;
            _httpBowQueryParser = httpDocumentQueryParser;
        }

        public void Dispose()
        {
        }

        public ResponseModel Read(string collectionName, HttpRequest request)
        {
            var timer = Stopwatch.StartNew();
            var collectionId = collectionName.ToHash();

            using (var session = _sessionFactory.CreateReadSession(collectionName, collectionId))
            {
                IList<IDictionary<string, object>> docs;
                long total;
                var stream = new MemoryStream();

                if (request.Query.ContainsKey("id"))
                {
                    var ids = request.Query["id"].Select(s => long.Parse(s));

                    docs = session.ReadDocs(ids);
                    total = docs.Count;

                    this.Log(string.Format("executed lookup by id in {0}", timer.Elapsed));
                }
                else
                {
                    var query = _httpQueryParser.Parse(collectionId, request);

                    if (query == null)
                    {
                        return new ResponseModel { MediaType = "application/json", Total = 0 };
                    }

                    var result = session.Read(query);

                    docs = result.Docs;
                    total = result.Total;

                    this.Log(string.Format("executed query {0} in {1}", query, timer.Elapsed));

                    if (request.Query.ContainsKey("create"))
                    {
                        var newCollectionName = request.Query["newCollection"].ToString();

                        if (string.IsNullOrWhiteSpace(newCollectionName))
                        {
                            newCollectionName = Guid.NewGuid().ToString();
                        }

                        _sessionFactory.Commit(new Job(newCollectionName, docs));
                    }
                }

                Serialize(docs, stream);

                return new ResponseModel
                {
                    MediaType = "application/json",
                    Stream = stream,
                    Documents = docs,
                    Total = total
                };
            }
        }

        private void Serialize(IList<IDictionary<string, object>> docs, Stream stream)
        {
            using (StreamWriter writer = new StreamWriter(stream))
            using (JsonTextWriter jsonWriter = new JsonTextWriter(writer))
            {
                JsonSerializer ser = new JsonSerializer();
                ser.Serialize(jsonWriter, docs);
                jsonWriter.Flush();
            }
        }
    }
}