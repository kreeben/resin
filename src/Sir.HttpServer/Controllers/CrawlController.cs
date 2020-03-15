using Microsoft.AspNetCore.Mvc;
using Sir.HttpServer.Features;
using System;

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
            ViewBag.CrawlId = Guid.NewGuid().ToString();

            return View();
        }

        [HttpPost]
        public IActionResult Post(
            string id, 
            string[] collection, 
            string[] field, 
            string q, 
            string job, 
            string and, 
            string or)
        {
            bool isValid = true;
            ViewBag.JobValidationError = null;
            ViewBag.TargetCollectionValidationError = null;

            if (string.IsNullOrWhiteSpace(job))
            {
                ViewBag.JobValidationError = "Please select a job to execute.";
                isValid = false;
            }

            if (!isValid)
            {
                ViewBag.Collection = collection;
                ViewBag.Field = field;
                ViewBag.Q = q;
                ViewBag.Job = job;

                return View("Index");
            }

            var jobType = job.ToLower();

            _crawlQueue.Enqueue(new CrawlJob(id, collection, field, q, job, and!=null, or!=null));

            return View(jobType);
        }
    }
}