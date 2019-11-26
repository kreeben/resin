using Microsoft.AspNetCore.Mvc;
using Sir.HttpServer.Features;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Sir.HttpServer.Controllers
{
    [Route("crawl")]
    public class CrawlController : UIController
    {
        private readonly CrawlQueue _crawlQueue;

        public CrawlController(
            IConfigurationProvider config, 
            ISessionFactory sessionFactory,
            CrawlQueue crawlQueue) : base(config, sessionFactory)
        {
            _crawlQueue = crawlQueue;
        }

        [HttpGet]
        public IActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public IActionResult Post(string[] collection, string[] field, string q, string target, string job)
        {
            bool isValid = true;
            ViewBag.JobValidationError = null;
            ViewBag.TargetCollectionValidationError = null;

            if (string.IsNullOrWhiteSpace(job))
            {
                ViewBag.JobValidationError = "Please select a job to execute.";
                isValid = false;
            }

            if (string.IsNullOrWhiteSpace(target))
            {
                ViewBag.TargetCollectionValidationError = "Please enter a name for your collection.";
                isValid = false;
            }

            if (!isValid)
            {
                ViewBag.Collection = collection;
                ViewBag.Field = field;
                ViewBag.Q = q;
                ViewBag.Target = target;
                ViewBag.Job = job;

                return View("Index");
            }

            var jobType = job.ToLower();

            _crawlQueue.Enqueue(new CrawlJob(collection, field, q, target, job));

            return View(jobType);
        }
    }
}
