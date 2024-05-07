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
        private readonly IConfigurationProvider _config;

        public HttpWriter(IConfigurationProvider config)
        {
            _config = config;
        }

        public void Write(HttpRequest request, IModel<string> model, IIndexReadWriteStrategy indexStrategy)
        {
            var documents = Deserialize<IEnumerable<Document>>(request.Body);
            var collectionId = request.Query["collection"].First().ToHash();
            var directory = _config.Get("data_dir");

            using (var database = new DocumentDatabase<string>(directory, collectionId, model, indexStrategy))
            {
                foreach(var document in documents)
                {
                    database.Write(document);
                }
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
    }
}