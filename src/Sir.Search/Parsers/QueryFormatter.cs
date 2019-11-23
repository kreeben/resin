using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace Sir.Search
{
    public class QueryFormatter : IQueryFormatter
    {
        private readonly SessionFactory _sessionFactory;

        public QueryFormatter(SessionFactory sessionFactory)
        {
            _sessionFactory = sessionFactory;
        }

        public string Format(HttpRequest request, IStringModel tokenizer)
        {
            var parser = new HttpQueryParser(new QueryParser(_sessionFactory, tokenizer));
            var query = parser.ParseRequest(request);
            var dictionary = new Dictionary<string, object>();
            
            parser.ParseQuery(query, dictionary);

            return JsonConvert.SerializeObject(dictionary, Formatting.Indented);
        }
    }
}
