using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Sir.HttpServer.Controllers
{
    [Route("query")]
    public class QueryController : Controller
    {
        private readonly PluginsCollection _plugins;
        private readonly IStringModel _model;
        private readonly ILogger<QueryController> _logger;

        public QueryController(PluginsCollection plugins, IStringModel tokenizer, ILogger<QueryController> logger)
        {
            _plugins = plugins;
            _model = tokenizer;
            _logger = logger;
        }

        [HttpGet]
        [HttpPost]
        public async Task<IActionResult> Get()
        {
            var mediaType = Request.Headers["Accept"].ToArray()[0];
            var reader = _plugins.Get<IHttpReader>(mediaType);

            if (reader == null)
            {
                reader = _plugins.Get<IHttpReader>("application/json");

                if (reader == null)
                {
                    throw new NotSupportedException(); // Media type not supported
                }
            }

            var timer = Stopwatch.StartNew();
            var result = await reader.Read(Request, _model);

            _logger.LogInformation($"processed {mediaType} request in {timer.Elapsed}");

            Response.Headers.Add("X-Total", result.Total.ToString());

            if (result.Total == 0)
                return new EmptyResult();

            return new FileContentResult(result.Body, result.MediaType);
        }
    }
}