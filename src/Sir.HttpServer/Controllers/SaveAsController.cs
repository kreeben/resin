using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Mvc;
using Sir.HttpServer.Features;
using Sir.Search;

namespace Sir.HttpServer.Controllers
{
    [Route("saveas")]
    public class SaveAsController : UIController
    {
        private readonly JobQueue _queue;
        private readonly IStringModel _model;
        private readonly QueryParser _queryParser;

        public SaveAsController(
            IConfigurationProvider config,
            SessionFactory sessionFactory,
            IStringModel model,
            QueryParser queryParser,
            SaveAsJobQueue queue) : base(config, sessionFactory)
        {
            _queue = queue;
            _model = model;
            _queryParser = queryParser;
        }

        [HttpGet]
        public IActionResult Index()
        {
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

            _queue.Enqueue(new SaveAsJob(
                SessionFactory,
                _queryParser,
                _model,
                SessionFactory.LoggerFactory.CreateLogger<SaveAsJob>(),
                id, 
                collection, 
                field, 
                q, 
                job, 
                and!=null, 
                or!=null));

            return View(jobType);
        }
    }
}