using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

namespace Sir.HttpServer.Controllers
{
    public abstract class UIController : Controller
    {
        private readonly ISessionFactory _sessionFactory;
        private IConfigurationProvider config;

        protected IConfigurationProvider Config { get; }

        public UIController(IConfigurationProvider config, ISessionFactory sessionFactory)
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
            ViewBag.DefaultCollection = Config.Get("default_collection");

            ViewBag.Collection = context.HttpContext.Request.Query.ContainsKey("collection") ?
                context.HttpContext.Request.Query["collection"].ToString() :
                ViewBag.DefaultCollection;

            base.OnActionExecuted(context);
        }
    }
}