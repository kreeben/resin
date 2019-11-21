using Microsoft.AspNetCore.Mvc;
using Sir.Search;

namespace Sir.HttpServer.Controllers
{
    public class HomeController : UIController
    {
        private readonly IConfigurationProvider _config;

        public HomeController(IConfigurationProvider config, ISessionFactory sessionFactory) : base(config, sessionFactory)
        {
            _config = config;
        }

        public ActionResult Index()
        {
            return View();
        }

        [Route("/about")]
        public ActionResult About()
        {
            return View();
        }

        [Route("/faq")]
        public ActionResult Faq()
        {
            return View();
        }

        [Route("/donate")]
        public ActionResult Donate()
        {
            return View();
        }

        [Route("/toc")]
        public ActionResult Toc()
        {
            return View();
        }

        [Route("/contact")]
        public ActionResult Contact()
        {
            return View();
        }
    }
}