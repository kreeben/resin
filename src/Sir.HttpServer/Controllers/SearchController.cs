using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Sir.Search;

namespace Sir.HttpServer.Controllers
{
    public class SearchController : UIController
    {
        private readonly PluginsCollection _plugins;
        private readonly IStringModel _model;

        public SearchController(
            PluginsCollection plugins, 
            IConfigurationProvider config, 
            IStringModel model,
            SessionFactory sessionFactory) : base(config, sessionFactory)
        {
            _plugins = plugins;
            _model = model;
        }

        [HttpGet("/search/")]
        [HttpPost("/search/")]
        public async Task<IActionResult> Index(string q)
        {
            if (string.IsNullOrWhiteSpace(q)) return View();

            var timer = new Stopwatch();
            timer.Start();

            ViewData["q"] = q;

            var reader = _plugins.Get<IHttpReader>("application/json");

            if (reader == null)
            {
                throw new System.NotSupportedException();
            }

            var result = await reader.Read(Request, _model);

            ViewData["time_ms"] = timer.ElapsedMilliseconds;
            ViewData["total"] = result.Total;

            if (result.Total == 0)
            {
                return View(new SearchResultModel[0]);
            }

            var documents = result.Documents.Select(x => new SearchResultModel { Document = x });

            return View(documents);
        }
    }

    public class SearchResultModel
    {
        public IDictionary<string, object> Document { get; set; }
    }
}