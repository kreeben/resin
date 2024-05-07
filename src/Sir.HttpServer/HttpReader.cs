using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
    public class HttpReader
    {
        private readonly ILogger<HttpReader> _logger;
        private readonly IConfigurationProvider _config;
        private readonly IModel<string> _model;
        private readonly IIndexReadWriteStrategy _strategy;

        public HttpReader(
            IModel<string> model,
            IIndexReadWriteStrategy strategy,
            IConfigurationProvider config,
            ILogger<HttpReader> logger)
        {
            _logger = logger;
            _config = config;
            _model = model;
            _strategy = strategy;
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

            var collection = request.Query["collection"].First();
            var collectionId = collection.ToHash();
            var directory = _config.Get("data_dir");

            using (var database = new DocumentDatabase<string>(directory, collectionId, _model, _strategy, _logger))
            {
                var httpQueryParser = new HttpQueryParser(database.CreateQueryParser());
                var query = await httpQueryParser.ParseRequest(request);

                if (query == null)
                {
                    return new SearchResult(null, 0, 0, new Document[0]);
                }

#if DEBUG
                var debug = new Dictionary<string, object>();

                httpQueryParser.ParseQuery(query, debug);

                var queryLog = JsonConvert.SerializeObject(debug);

                _logger.LogDebug($"parsed query: {queryLog}");
#endif

                return database.Read(query, skip, take);
            }
        }
    }
}