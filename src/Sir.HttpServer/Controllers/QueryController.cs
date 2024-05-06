using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.Documents;

namespace Sir.HttpServer.Controllers
{
    [Route("query")]
    public class QueryController : Controller
    {
        private readonly IHttpReader _reader;
        private readonly IModel<string> _model;
        private readonly ILogger<QueryController> _logger;

        public QueryController(IHttpReader reader, IModel<string> model, ILogger<QueryController> logger)
        {
            _reader = reader;
            _model = model;
            _logger = logger;
        }

        [HttpGet]
        [HttpPost]
        public async Task<IActionResult> Get()
        {
            var timer = Stopwatch.StartNew();
            var result = await _reader.Read(Request, _model);

            _logger.LogInformation($"processed request in {timer.Elapsed}");

            Response.Headers.Add("X-TotalDocumentCount", result.Total.ToString());

            if (result.Total == 0)
                return new EmptyResult();

            using (var mem = new MemoryStream())
            {
                Serialize(result.Documents, mem);

                return new FileContentResult(mem.ToArray(), "application/json");
            }
        }

        private void Serialize(IEnumerable<Document> docs, Stream stream)
        {
            using (StreamWriter writer = new StreamWriter(stream))
            using (JsonTextWriter jsonWriter = new JsonTextWriter(writer))
            {
                JsonSerializer ser = new JsonSerializer();
                ser.Serialize(jsonWriter, docs);
                jsonWriter.Flush();
            }
        }
    }
}