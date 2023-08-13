using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.IO;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Sir.HttpServer
{
    public class StringQueryFormatter : IQueryFormatter<string>
    {
        private readonly KeyRepository _keys;
        private readonly ILogger _log;
        private readonly string _directory;

        public StringQueryFormatter(string directory, KeyRepository keys, ILogger log)
        {
            _keys = keys;
            _log = log;
            _directory = directory;
        }

        public async Task<string> Format(HttpRequest request, IModel<string> tokenizer)
        {
            var parser = new HttpQueryParser(new QueryParser<string>(_directory, _keys, tokenizer, logger: _log));
            var query = await parser.ParseRequest(request);
            var dictionary = new Dictionary<string, object>();
            
            parser.ParseQuery(query, dictionary);

            return JsonConvert.SerializeObject(dictionary, Formatting.Indented);
        }
    }
}