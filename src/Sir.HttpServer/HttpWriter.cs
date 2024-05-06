using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Sir.Documents;

namespace Sir.HttpServer
{
    /// <summary>
    /// Write to a collection.
    /// </summary>
    public class HttpWriter : IHttpWriter
    {
        private readonly SessionFactory _sessionFactory;
        private readonly IConfigurationProvider _config;

        public HttpWriter(SessionFactory sessionFactory, IConfigurationProvider config)
        {
            _sessionFactory = sessionFactory;
            _config = config;
        }

        public void Write(HttpRequest request, IModel<string> model, IIndexReadWriteStrategy indexStrategy)
        {
            var documents = Deserialize<IEnumerable<Document>>(request.Body);
            var collectionId = request.Query["collection"].First().ToHash();

            _sessionFactory.StoreDataAndPersistIndex(
                _config.Get("data_dir"),
                collectionId,
                documents,
                model,
                indexStrategy);
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
    }
}