using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Sir.Search
{
    /// <summary>
    /// Query a collection.
    /// </summary>
    public class HttpReader : IHttpReader
    {
        public string ContentType => "application/json";

        private readonly ILogger<HttpReader> _logger;
        private readonly SessionFactory _sessionFactory;
        private readonly HttpQueryParser _httpQueryParser;
        private readonly IConfigurationProvider _config;

        public HttpReader(
            SessionFactory sessionFactory, 
            HttpQueryParser httpQueryParser,
            IConfigurationProvider config,
            ILogger<HttpReader> logger)
        {
            _logger = logger;
            _sessionFactory = sessionFactory;
            _httpQueryParser = httpQueryParser;
            _config = config;
        }

        public void Dispose()
        {
        }

        public async Task<ResponseModel> Read(HttpRequest request, IStringModel model)
        {
            var timer = Stopwatch.StartNew();
            var take = 100;
            var skip = 0;

            if (request.Query.ContainsKey("take"))
                take = int.Parse(request.Query["take"]);

            if (request.Query.ContainsKey("skip"))
                skip = int.Parse(request.Query["skip"]);

            var query = await _httpQueryParser.ParseRequest(request);

            if (query == null)
            {
                return new ResponseModel { MediaType = "application/json", Total = 0 };
            }

#if DEBUG

            var debug = new Dictionary<string, object>();

            _httpQueryParser.ParseQuery(query, debug);

            _logger.LogInformation(JsonConvert.SerializeObject(debug));
            _logger.LogInformation($"divider {query.GetDivider()}");

#endif
            ReadResult result = null;

            using (var readSession = _sessionFactory.CreateReadSession())
            {
                if (request.Query.ContainsKey("id") && request.Query.ContainsKey("collection"))
                {
                    var collectionId = request.Query["collection"].ToString().ToHash();
                    var ids = request.Query["id"].ToDictionary(s => (collectionId, docId:long.Parse(s)), x => (double)1);
                    var docs = readSession.ReadDocs(ids, query);

                    result = new ReadResult { Query = query, Docs = docs, Total = docs.Count };
                }
                else
                {
                    result = readSession.Read(query, skip, take);
                }
            }

            using (var mem = new MemoryStream())
            {
                Serialize(result.Docs, mem);

                return new ResponseModel
                {
                    MediaType = "application/json",
                    Documents = result.Docs,
                    Total = result.Total,
                    Body = mem.ToArray()
                };
            }
        }

        private void Serialize(IEnumerable<IDictionary<string, object>> docs, Stream stream)
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