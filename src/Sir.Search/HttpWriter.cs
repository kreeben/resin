using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;

namespace Sir.Search
{
    /// <summary>
    /// Write into a collection.
    /// </summary>
    public class HttpWriter : IHttpWriter
    {
        public string ContentType => "application/json";

        private readonly SessionFactory _sessionFactory;
        private readonly Stopwatch _timer;

        public HttpWriter(SessionFactory sessionFactory)
        {
            _sessionFactory = sessionFactory;
            _timer = new Stopwatch();
        }

        public void Write(HttpRequest request, IStringModel model)
        {
            var documents = Deserialize<IEnumerable<IDictionary<string, object>>>(request.Body);
            var storedFieldNames = new HashSet<string>(request.Query["storedFields"].ToArray());
            var indexedFieldNames = new HashSet<string>(request.Query["indexedFields"].ToArray());

            if (request.Query.ContainsKey("collection"))
            {
                var collections = request.Query["collection"].ToArray();
                
                foreach (var collection in collections)
                {
                    _sessionFactory.Write(
                        new WriteJob(
                            collection.ToHash(), 
                            documents, 
                            model,
                            storedFieldNames,
                            indexedFieldNames));
                }
            }
            else
            {
                _sessionFactory.Write(
                    documents, 
                    model, 
                    storedFieldNames, 
                    indexedFieldNames);
            }
        }

        private static T Deserialize<T>(Stream stream)
        {
            using (StreamReader reader = new StreamReader(stream))
            using (JsonTextReader jsonReader = new JsonTextReader(reader))
            {
                JsonSerializer ser = new JsonSerializer();
                return ser.Deserialize<T>(jsonReader);
            }
        }

        public void Dispose()
        {
        }
    }
}