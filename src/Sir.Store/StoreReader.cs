﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
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
        private readonly StoreWriter _storeWriter;
        private readonly ITokenizer _tokenizer;

        public StoreReader(
            SessionFactory sessionFactory, 
            HttpQueryParser httpQueryParser, 
            HttpBowQueryParser httpDocumentQueryParser, 
            ITokenizer tokenizer, 
            IEnumerable<IWriter> storeWriters)
        {
            _sessionFactory = sessionFactory;
            _httpQueryParser = httpQueryParser;
            _tokenizer = tokenizer;
            _httpBowQueryParser = httpDocumentQueryParser;
            
            foreach (var writer in storeWriters)
            {
                if (writer is StoreWriter)
                {
                    _storeWriter = (StoreWriter)writer;
                    break;
                }
            }
        }

        public void Dispose()
        {
        }

        public async Task<ResponseModel> Read(string collectionName, HttpRequest request)
        {
            var timer = Stopwatch.StartNew();
            var collectionId = collectionName.ToHash();
            var vec1FileName = Path.Combine(_sessionFactory.Dir, string.Format("{0}.vec1", collectionId));

            if (File.Exists(vec1FileName))
            {
                Query query;

                using (var mapSession = _sessionFactory.CreateReadSession(collectionName, collectionId))
                {
                    query = _httpBowQueryParser.Parse(collectionId, request, mapSession);
                }

                using (var readSession = _sessionFactory.CreateReadSession(collectionName, collectionId, "ix1", "ixp1", "vec1"))
                {
                    var result = await readSession.Read(query);

                    this.Log(
                        string.Format(
                            "executed query {0} and read {1} docs from disk in {2}",
                            query,
                            result.Docs.Count,
                            timer.Elapsed));

                    var stream = new MemoryStream();

                    Serialize(result.Docs, stream);

                    return new ResponseModel
                    {
                        MediaType = "application/json",
                        Stream = stream,
                        Documents = result.Docs,
                        Total = result.Total
                    };
                }

            }
            else
            {
                using (var session = _sessionFactory.CreateReadSession(collectionName, collectionId))
                {
                    IList<IDictionary> docs;
                    long total;
                    var stream = new MemoryStream();

                    if (request.Query.ContainsKey("id"))
                    {
                        var ids = request.Query["id"].Select(s => long.Parse(s));

                        docs = await session.ReadDocs(ids);
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

                        var result = await session.Read(query);

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

                            await _storeWriter.ExecuteWrite(newCollectionName, docs);
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
        }

        private void Serialize(IList<IDictionary> docs, Stream stream)
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