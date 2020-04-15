using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Sir.Document;
using Sir.Search;
using System.IO;

namespace Sir.HttpServer.Controllers
{
    public abstract class UIController : Controller
    {
        private readonly SessionFactory _sessionFactory;
        private IConfigurationProvider config;

        protected IConfigurationProvider Config { get; }
        protected SessionFactory SessionFactory => _sessionFactory;

        public UIController(IConfigurationProvider config, SessionFactory sessionFactory)
        {
            Config = config;
            _sessionFactory = sessionFactory;
        }

        protected UIController(IConfigurationProvider config)
        {
            this.config = config;
        }

        public override void OnActionExecuted(ActionExecutedContext context)
        {
            ViewBag.CCTargetUrl = Config.Get("cc_target_url");
            ViewBag.CCTargetName = Config.Get("cc_target_name");
            ViewBag.DefaultCollection = Config.Get("default_collection").Split(',', System.StringSplitOptions.RemoveEmptyEntries);

            ViewBag.Collection = context.HttpContext.Request.Query.ContainsKey("collection") ?
                context.HttpContext.Request.Query["collection"].ToArray() :
                ViewBag.DefaultCollection;

            var dixFileName = Path.Combine(_sessionFactory.Dir, $"{"cc_wat".ToHash()}.dix");

            if (System.IO.File.Exists(dixFileName))
            {
                using (var dixFile = _sessionFactory.CreateReadStream(dixFileName))
                using (var dix = new DocIndexReader(dixFile))
                {
                    ViewBag.DocumentCount = dix.Count;
                }
            }
            
            base.OnActionExecuted(context);
        }
    }
}