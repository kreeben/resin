using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Mvc;
using Sir.HttpServer.Features;
using Sir.Search;
using System.Collections.Generic;

namespace Sir.HttpServer.Controllers
{
    [Route("saveas")]
    public class SaveAsController : UIController
    {
        private readonly IStringModel _model;
        private readonly QueryParser _queryParser;
        private readonly ILogger<SaveAsController> _log;
        private static readonly HashSet<string> _reservedCollections = new HashSet<string> { "cc_wat", "cc_wet" };

        public SaveAsController(
            IConfigurationProvider config,
            SessionFactory sessionFactory,
            IStringModel model,
            QueryParser queryParser,
            SaveAsJobQueue queue,
            ILogger<SaveAsController> log) : base(config, sessionFactory)
        {
            _model = model;
            _queryParser = queryParser;
            _log = log;
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
            string[] select,
            string q,
            string target,
            string truncate,
            string and, 
            string or,
            int skip,
            int take)
        {
            bool isValid = true;
            ViewBag.JobValidationError = null;
            ViewBag.TargetCollectionValidationError = null;

            if (string.IsNullOrWhiteSpace(target))
            {
                ViewBag.JobValidationError = "Please choose a collection name.";
                isValid = false;
            }
            else if (_reservedCollections.Contains(target))
            {
                ViewBag.JobValidationError = "That collection is read-only. Please choose a valid collection name.";
                isValid = false;
            }

            if (!isValid)
            {
                ViewBag.Collection = collection;
                ViewBag.Field = field;
                ViewBag.Q = q;

                return View("Index");
            }

            new SaveAsJob
                (
                    sessionFactory: SessionFactory,
                    queryParser: _queryParser,
                    model: _model,
                    logger: _log,
                    target: target,
                    collections: collection,
                    fields: field,
                    select: select,
                    q: q,
                    and: and != null,
                    or: or != null,
                    skip: skip,
                    take: take,
                    truncate != null
                ).Execute();

            ViewBag.Target = target;

            return View("Saved");
        }
    }
}