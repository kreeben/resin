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
            JobQueue queue) : base(config, sessionFactory)
        {
            _queue = queue;
            _model = model;
            _queryParser = queryParser;
        }

        [HttpGet("ccc")]
        public IActionResult CCC(
            string crawlid)
        {
            if (crawlid is null)
            {
                throw new ArgumentNullException(nameof(crawlid));
            }

            ViewBag.CrawlId = crawlid;

            var status = _queue.GetStatus(crawlid);

            if (status != null)
            {
                ViewBag.DownloadStatus = Math.Min(100, Convert.ToInt32(status["download"]));
                ViewBag.IndexStatus = Math.Min(100, Convert.ToInt32(status["index"]));
            }

            if (status == null || (ViewBag.DownloadStatus == 100 && ViewBag.IndexStatus == 100))
            {
                return View("ccc_done");
            }

            return View();
        }
    }
}