using Sir.Documents;
using System;

namespace Sir
{
    public interface ISearchSession : IDisposable
    {
        SearchResult Search(IQuery query, int skip, int take);
        Document SearchScalar(IQuery query);
    }
}