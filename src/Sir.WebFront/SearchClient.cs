using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.IO;

namespace Sir.HttpServer
{
    /// <summary>
    /// Query a collection.
    /// </summary>
    public class SearchClient
    {
        private readonly ILogger<SearchClient> _logger;
        private readonly SessionFactory _sessionFactory;
        private readonly HttpQueryParser _httpQueryParser;
        private readonly IConfigurationProvider _config;
        private readonly string[] _fields;
        private readonly string[] _select;

        public SearchClient(
            SessionFactory sessionFactory,
            HttpQueryParser httpQueryParser,
            IConfigurationProvider config,
            ILogger<SearchClient> logger)
        {
            _logger = logger;
            _sessionFactory = sessionFactory;
            _httpQueryParser = httpQueryParser;
            _config = config;
            _fields = _config.GetMany("default_fields");
            _select = _config.GetMany("default_select_fields");
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

            var queryId = request.Query["queryId"].ToString();
            var userDirectory = Path.Combine(_config.Get("user_dir"), queryId);
            var urlCollectionId = "url".ToHash();
            var collections = new List<string>();
            var directory = _config.Get("data_dir");
            var keys = new KeyRepository(directory, _sessionFactory);

            using (var documentReader = new DocumentStreamSession(_sessionFactory, keys, userDirectory))
            {
                foreach (var url in documentReader.ReadDocumentValues<string>(urlCollectionId, "host"))
                {
                    collections.Add(url);
                }
            }

            var query = await _httpQueryParser.ParseRequest(request, collections, _fields, _select);

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

            using (var readSession = new SearchSession(_config.Get("data_dir"), keys, _sessionFactory, model, new LogStructuredIndexingStrategy(model), _logger))
            {
                return readSession.Search(query, skip, take);
            }
        }

        public void Dispose()
        {
        }
    }

    public class StringUtil
    {
        public static string Base64Encode(string plainText)
        {
            var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(plainText);
            return System.Convert.ToBase64String(plainTextBytes);
        }

        public static string Base64Decode(string base64EncodedData)
        {
            var base64EncodedBytes = System.Convert.FromBase64String(base64EncodedData);
            return System.Text.Encoding.UTF8.GetString(base64EncodedBytes);
        }
    }
}