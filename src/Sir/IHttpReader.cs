using Microsoft.AspNetCore.Http;
using System.Threading.Tasks;

namespace Sir
{
    /// <summary>
    /// Read data from a collection.
    /// </summary>
    public interface IHttpReader : IPlugin
    {
       Task<ResponseModel> Read(HttpRequest request, IStringModel model);
    }
}
