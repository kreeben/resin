using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.Documents;
using Sir.KeyValue;

namespace Sir.HttpServer
{
    /// <summary>
    /// Query a collection.
    /// </summary>
    public class HttpReader : IHttpReader
    {
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

        public async Task<SearchResult> Read(HttpRequest request, IModel<string> model)
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
                return new SearchResult(null, 0, 0, new Document[0]);
            }

#if DEBUG
            var debug = new Dictionary<string, object>();

            _httpQueryParser.ParseQuery(query, debug);

            var queryLog = JsonConvert.SerializeObject(debug);

            _logger.LogDebug($"parsed query: {queryLog}");
#endif

            using (var readSession = new SearchSession(_config.Get("data_dir"), model, new LogStructuredIndexingStrategy(model), new KeyValueReader(_config.Get("data_dir"), _config.Get("default_collection").ToHash()), _logger))
            {
                return readSession.Search(query, skip, take);
            }
        }
    }
}