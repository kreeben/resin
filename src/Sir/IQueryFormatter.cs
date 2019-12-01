using Microsoft.AspNetCore.Http;
using System.Threading.Tasks;

namespace Sir
{
    public interface IQueryFormatter
    {
        Task<string> Format(HttpRequest request, IStringModel model);
    }
}