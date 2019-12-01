using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Sir.Search
{
    public class QueryFormatter : IQueryFormatter
    {
        private readonly SessionFactory _sessionFactory;

        public QueryFormatter(SessionFactory sessionFactory)
        {
            _sessionFactory = sessionFactory;
        }

        public async Task<string> Format(HttpRequest request, IStringModel tokenizer)
        {
            var parser = new HttpQueryParser(new QueryParser(_sessionFactory, tokenizer));
            var query = await parser.ParseRequest(request);
            var dictionary = new Dictionary<string, object>();
            
            parser.ParseQuery(query, dictionary);

            return JsonConvert.SerializeObject(dictionary, Formatting.Indented);
        }
    }
}
