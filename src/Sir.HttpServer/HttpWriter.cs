using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Sir.Documents;

namespace Sir.HttpServer
{
    /// <summary>
    /// Write to a collection.
    /// </summary>
    public class HttpWriter
    {
        private readonly IConfigurationProvider _config;

        public HttpWriter(IConfigurationProvider config)
        {
            _config = config;
        }

        public async Task Write(HttpRequest request, IModel<string> model, IIndexReadWriteStrategy indexStrategy)
        {
            var documents = await Deserialize<IEnumerable<Document>>(request.Body);
            var collectionId = request.Query["collection"].First().ToHash();
            var directory = _config.Get("data_dir");

            using (var database = new DocumentDatabase<string>(directory, collectionId, model, indexStrategy))
            {
                foreach (var document in documents)
                {
                    database.Write(document);
                }
            }
        }

        private static async Task<T> Deserialize<T>(Stream stream)
        {
            using (var sr = new StreamReader(stream))
            {
                var json = await sr.ReadToEndAsync();
                return JsonConvert.DeserializeObject<T>(json);
            }

            //using (StreamReader reader = new StreamReader(stream))
            //using (JsonTextReader jsonReader = new JsonTextReader(reader))
            //{
            //    JsonSerializer ser = new JsonSerializer();
            //    return ser.Deserialize<T>(jsonReader);
            //}
        }
    }
}