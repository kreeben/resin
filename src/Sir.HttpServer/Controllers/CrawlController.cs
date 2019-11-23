using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Sir.HttpServer.Controllers
{
    [Route("crawl")]
    public class CrawlController : UIController
    {
        public CrawlController(IConfigurationProvider config, ISessionFactory sessionFactory) : base(config, sessionFactory)
        {
        }

        [HttpGet]
        public IActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public IActionResult Post(string[] collection, string[] field, string q, string target, string job)
        {
            var jobType = job.ToLower();

            return View(jobType);

            
        }
    }
}
