using System;
using System.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Sir.HttpServer.Controllers
{
    [Route("write")]
    public class WriteController : Controller
    {
        private readonly PluginsCollection _plugins;
        private readonly IStringModel _model;
        private readonly ILogger<WriteController> _logger;

        public WriteController(PluginsCollection plugins, IStringModel tokenizer, ILogger<WriteController> logger)
        {
            _plugins = plugins;
            _model = tokenizer;
            _logger = logger;
        }

        [HttpPost]
        [DisableRequestSizeLimit]
        public IActionResult Post(string accessToken)
        {
            if (!IsValidToken(accessToken))
            {
                return StatusCode((int)HttpStatusCode.MethodNotAllowed);
            }

            if (string.IsNullOrWhiteSpace(Request.ContentType))
            {
                throw new NotSupportedException();
            }

            var writer = _plugins.Get<IHttpWriter>(Request.ContentType);

            if (writer == null)
            {
                writer = _plugins.Get<IHttpWriter>(Request.ContentType.Split(';', StringSplitOptions.RemoveEmptyEntries)[0]);
            }

            if (writer == null)
            {
                throw new NotSupportedException(); // Media type not supported
            }

            try
            {
                writer.Write(Request, _model);

                return Ok();
            }
            catch (Exception ew)
            {
                _logger.LogError(ew.ToString());

                throw ew;
            }
        }

        private bool IsValidToken(string accessToken)
        {
            if (string.IsNullOrWhiteSpace(accessToken))
                return false;

            return "Pruttapa1!".Equals(accessToken);
        }
    }
}