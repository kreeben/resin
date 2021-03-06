﻿using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Sir.Documents;
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
            ViewBag.DefaultFields = Config.Get("default_fields").Split(',', System.StringSplitOptions.RemoveEmptyEntries);
            ViewBag.Collection = context.HttpContext.Request.Query.ContainsKey("collection") ?
                context.HttpContext.Request.Query["collection"].ToArray() :
                ViewBag.DefaultCollection;
           
            base.OnActionExecuted(context);
        }
    }
}