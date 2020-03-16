using Microsoft.AspNetCore.Mvc;
using Sir.Search;

namespace Sir.HttpServer.Controllers
{
    public class UpvoteController : UIController
    {
        public UpvoteController(IConfigurationProvider config, SessionFactory sessionFactory) : base(config, sessionFactory)
        {
        }

        [HttpGet("upvote")]
        public ActionResult Index(string collection, string q, string url, string OR, string AND, string[] fields)
        {
            ViewData["url"] = url;
            ViewData["q"] = q;

            return Redirect(Request.Headers["referer"]);
        }
    }
}