using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Mvc;
using Sir.HttpServer.Features;
using Sir.Search;
using System;

namespace Sir.HttpServer.Controllers
{
    [Route("status")]
    public class StatusController : UIController
    {
        private readonly JobQueue _queue;
        private readonly IStringModel _model;
        private readonly QueryParser _queryParser;

        public StatusController(
            IConfigurationProvider config,
            SessionFactory sessionFactory,
            IStringModel model,
            QueryParser queryParser,
            CrawlJobQueue queue) : base(config, sessionFactory)
        {
            _queue = queue;
            _model = model;
            _queryParser = queryParser;
        }

        [HttpGet("ccc")]
        public IActionResult CCC(string crawlid)
        {
            string header;
            string message;

            if (_queue.IsQueued(crawlid) && !_queue.IsProcessed(crawlid))
            {
                header = "Downloading and indexing WET file...";
                message = "";
                ViewBag.IsDone = false;
            }
            else
            {
                header = $"Downloading and indexing WET file is done!";
                message = "cc_wet has been enriched.";
                ViewBag.IsDone = true;
            }

            ViewBag.Header = $"{header}";
            ViewBag.Message = $"{message}";
            ViewBag.CrawlId = crawlid;

            return View();
        }
    }
}