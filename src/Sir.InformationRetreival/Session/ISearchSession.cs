using Sir.Documents;
using System;

namespace Sir
{
    public interface ISearchSession : IDisposable
    {
        SearchResult Search(Query query, int skip, int take);
        Document SearchScalar(Query query);
    }
}