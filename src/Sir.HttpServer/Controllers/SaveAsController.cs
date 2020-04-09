using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Mvc;
using Sir.HttpServer.Features;
using Sir.Search;

namespace Sir.HttpServer.Controllers
{
    [Route("saveas")]
    public class SaveAsController : UIController
    {
        private readonly IStringModel _model;
        private readonly QueryParser _queryParser;

        public SaveAsController(
            IConfigurationProvider config,
            SessionFactory sessionFactory,
            IStringModel model,
            QueryParser queryParser,
            SaveAsJobQueue queue) : base(config, sessionFactory)
        {
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
            string[] collection, 
            string[] field, 
            string q,
            string target,
            string and, 
            string or)
        {
            bool isValid = true;
            ViewBag.JobValidationError = null;
            ViewBag.TargetCollectionValidationError = null;

            if (string.IsNullOrWhiteSpace(target))
            {
                ViewBag.JobValidationError = "Please choose a collection name.";
                isValid = false;
            }

            if (!isValid)
            {
                ViewBag.Collection = collection;
                ViewBag.Field = field;
                ViewBag.Q = q;

                return View("Index");
            }

            new SaveAsJob(
                SessionFactory,
                _queryParser,
                _model,
                SessionFactory.LoggerFactory.CreateLogger<SaveAsJob>(),
                new System.Collections.Generic.HashSet<string>(field),
                target,
                collection,
                field,
                q,
                and != null,
                or != null).Execute();

            return View(target);
        }
    }
}