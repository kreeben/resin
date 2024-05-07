using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.Documents;

namespace Sir.HttpServer
{
    /// <summary>
    /// Query a collection.
    /// </summary>
    public class HttpReader : IHttpReader
    {
        private readonly ILogger<HttpReader> _logger;
        private readonly HttpQueryParser _httpQueryParser;
        private readonly IConfigurationProvider _config;

        public HttpReader(
            HttpQueryParser httpQueryParser,
            IConfigurationProvider config,
            ILogger<HttpReader> logger)
        {
            _logger = logger;
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

            var directory = _config.Get("data_dir");

            using (var readSession = new SearchSession<string>(directory, model, new LogStructuredIndexingStrategy(model), _logger))
            {
                return readSession.Search(query, skip, take);
            }
        }
    }
}